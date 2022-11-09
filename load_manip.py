# ---------------------------------------------------------------
# Functions to load and manipulate data
# ----------------------------------------------------------------

import numpy as np
import xarray as xr
import pandas as pd
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
): 
    
    yi = f.split('_')[-2]
    yf = f.split('_')[-1].split('.')[0]
    
    if len(yf) > 4: # exception here for PIC YYYYMMDD-YYYYMMDD format
        
        yi = yf[:4]
        
    try:
        
        da = xr.open_dataarray(f, decode_times=False)
        
    except:
        
        da = xr.open_dataset(f, decode_times=False).exposure
    
    t = pd.date_range(
        start=str(yi),
        periods=da.sizes['time'],
        freq='D',
    )
    
    da['time'] = t # set time
    
    da = da.convert_calendar('365_day') # convert time to remove leap years
    
    return da

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
        
        # files = glob.glob('./{}/*tasmax_*.nc'.format(i))
        files = glob.glob('tasmax_*')
        # data[i] = []
        
        for f in files:
            
            # data[i].append(open_dataarray_isimip(f))
            data[i] = open_dataarray_isimip(f)

        # data[i] = xr.concat(data[i],dim='time')
        
    ds = xr.Dataset(
        coords={
            'lat' : ('lat',data[i].lat.data),
            'lon' : ('lon',data[i].lon.data),
            'time' : ('time',data[i].time.data),
        }
    )
    
    for i in ids:
        
        ds[i] = data[i]
        
        
    # for gcms, add final data var ('all') which is all 4 GCM's pic data concatenated with 0->len(all) time dimension
    # if ids == gcms:
        
    #     ds['all'] = xr.concat(
    #         [ds[gcm].assign_coords({'time':np.arange(i*len(ds[gcm].time),(i+1)*len(ds[gcm].time))}) for i,gcm in enumerate(gcms)],
    #         dim='time',
    #     )
    
    return ds

#%% ----------------------------------------------------------------   
# process PIC stats
def hwmid_qntls(
    ds,
): 
    
    # ds = ds.convert_calendar('365_day') # remove leap days (takes very long)
    
    for da in ds.data_vars:
        
        ds[da+'_75'] = ds[da].groupby('time.year').max('time').quantile(
            q=0.75,
            dim='year',
            method='inverted_cdf',
        )

        ds[da+'_25'] = ds[da].groupby('time.year').max('time').quantile(
            q=0.25,
            dim='year',
            method='inverted_cdf',
        )
        
        # place holder array for 90% qntl in 31 day windows per day (each calender day will be filled with this qntl)
        ds[da+'_90'] = xr.ones_like(ds[da]).groupby('time.dayofyear').mean('time')
        
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
            ] = ds[da].sel(time=ds['time.dayofyear'].isin(w)).quantile(
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
                    str(y),
                    periods=da_90.sizes['dayofyear'],
                    freq='D'
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


        
                
