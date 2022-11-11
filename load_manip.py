# ---------------------------------------------------------------
# Functions to load and manipulate data
# ----------------------------------------------------------------

import numpy as np
import xarray as xr
import dask
import pandas as pd
import datetime as dt
import geopandas as gpd
import pickle as pk
from scipy import interpolate
import regionmask
import glob
import os

from settings import *
init()

# #%% ----------------------------------------------------------------
def groupingfunc(da):
    da = xr.where(da.notnull(),len(da[~np.isnan(da)]),np.nan)
    return da

#%% ----------------------------------------------------------------   
# read netcdf
def open_dataarray_isimip(
    f,
    yi,
    yf,
): 

    ds = xr.open_mfdataset(
        f,
        chunks = {
            'lat':10,
            'lon':10,
            'time':None,
        },
        decode_times=False
    )

    t = xr.cftime_range(
        start=yi, 
        periods=ds.sizes['time'], 
        freq='D', 
        calendar='standard',
    )

    ds['time'] = t # set time

    dask.config.set({"array.slicing.split_large_chunks": True}) # this is setting time chunking to 126, which gives problems with groupby('time.year')
    ds = ds.convert_calendar('365_day')   
    ds = ds.chunk({'time':440}) # fix to chunk annually after the config.set chunking ; 265*36*72 elements is just <1,000,000
    
    return ds

#%% ----------------------------------------------------------------   
# open PIC or obs (ids is array of string identifiers; gcms or obs types)
def collect_arrays(
    ids,
    dir,
): 
    # global gcms
    os.chdir(dir)
    
    data = {}
    
    for i in ids:
        
        files = glob.glob('./{}/*tasmax_*.nc4'.format(i))
        files.sort()
        data[i] = []
        
        for f in files:
            
            yi = f.split('_')[-2]
            yf = f.split('_')[-1].split('.')[0]

            if len(yf) > 4: # exception here for PIC YYYYMMDD-YYYYMMDD format

                yi = yf[:4]
            
            if int(yi) <= 2091: # limiting data to 2099
                
                data[i].append(open_dataarray_isimip(f,yi,yf))

        data[i] = xr.concat(data[i],dim='time')
        
        ds = xr.merge([data[i].rename({'tasmax':'tasmax_{}'.format(i)}) for i in ids])
    
    return ds

#%% ----------------------------------------------------------------   
# process PIC stats
def hwmid_qntls(
    ds,
): 
    
    for da in ds.data_vars:
        
        da_max = ds['tasmax_{}'.format(da)].groupby('time.year').max('time').chunk({'year':-1})
        
        ds[da+'_75'] = da_max.quantile(
            q=0.75,
            dim='year',
            method='inverted_cdf',
        )

        ds[da+'_25'] = da_max.quantile(
            q=0.25,
            dim='year',
            method='inverted_cdf',
        )
        
        # place holder array for 90% qntl in 31 day windows per day (each calender day will be filled with this qntl)
        ds[da+'_90'] = xr.ones_like(ds['tasmax_{}'.format(da)]).isel(time=slice(0,365)).groupby('time.dayofyear').mean('time')
        
        # run window stats on each day
        for d in np.arange(1,366):
            
            i = d-15 # window start
            f = d+15 # window end
            w = np.arange(i,f+1) # window in calendar integers
            
            if np.any(w<0): # incase negative values
                
                w = np.where(
                    w>0,
                    w,
                    365+w,
                )
                
            elif np.any(w>365): # incase values over 365
                
                w = np.where(
                    w<=365,
                    w,
                    w-365,
                )                
            
            # per calendar day, d, assign 90% quantile from original array using window, w
            ds[da+'_90'].loc[
                {'dayofyear':d}
            ] = ds['tasmax_{}'.format(da)].sel(time=ds['time.dayofyear'].isin(w)).quantile(
                    q=0.9,
                    dim='time',
                    method='inverted_cdf',
                )
    
    return ds

#%% ----------------------------------------------------------------   
# hot period
def hot_period(
    da_t,
    da_25,
    da_75,
    da_90,
):
    
    # make 90% comparable by repeating val across years of da_t and mimicking time dim
    da_90_cmp = xr.concat(
        [
            da_90.assign_coords({'dayofyear':pd.date_range(
                    str(y),periods=da_90.sizes['dayofyear'],freq='D'
                # )}).rename({'dayofyear':'time'}).convert_calendar('365_day') for y in np.unique(da_t['time.year'].values)
                )}).rename({'dayofyear':'time'}) for y in np.unique(da_t['time.year'].values)
        ],
        dim='time',
    )
    
    # step 1, get hot days
    da_t['time'] = da_90_cmp['time'] # make calendars the same
    da_hd = xr.where(
        da_t>da_90_cmp,
        1,
        np.nan,
    ) 
    
    # step 2, get hot periods
    da_hd = da_hd.groupby(da_hd.isnull().cumsum(dim='time')).map(groupingfunc) # group between nans via cumsum of 1s, map vals as len of valid points in group 
    da_hp = xr.where(da_hd>=3,1,np.nan) # keep only groups with at least 3 days (hot periods)
    da_hp = da_t.where(da_hp==1)
    
    # step 3, calculate magnitudes 
    da_mgt = magnitude(
        da_hp,
        da_25,
        da_75,
    )
        
    return da_mgt

#%% ----------------------------------------------------------------   
# magnitude of hot period
def magnitude(
    da,
    da_25,
    da_75
):
    
    # run calc w.r.t. pic annual thresholds
    mgt = (da - da_25)/\
        (da_75 - da_25)
        
    # magnitudes for daily temps below 25% of PIC set to 0
    mgt = xr.where(da > da_25, mgt, np.nan)
    
    # sum magnitudes per hot period
    mgt = mgt.groupby(mgt.isnull().cumsum(dim='time')).sum(dim='time')
    
    # get highest per year
    mgt = mgt.groupby('time.year').max('time')
        
    return mgt


        
                
