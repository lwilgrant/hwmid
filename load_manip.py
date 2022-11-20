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
import time
import settings
gcms, obs_types, pdir, cdir, lat_chunk, lon_chunk, time_chunk = settings.init()

#%% ----------------------------------------------------------------
def groupingfunc(da):
    da = xr.where(da.notnull(),len(da[~np.isnan(da)]),np.nan)
    return da

#%% ----------------------------------------------------------------
def year_max(da):
    gb = da.groupby('time.year')
    return gb.max('time').chunk({'year':-1})

#%% ----------------------------------------------------------------
def multiyr_window_pctl(da,w,d):
    return da.sel(time=da['time.dayofyear'].isin(w)).chunk({'time':-1}).quantile(q=0.9,dim='time',method='inverted_cdf').assign_coords({'dayofyear':d})

#%% ----------------------------------------------------------------   
# read netcdf
def open_daily_array(
    f,
    yi,
): 

    ds = xr.open_mfdataset(
        f,
        chunks = {
            'lat':lat_chunk,
            'lon':lon_chunk,          
            'time':time_chunk,
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

    dask.config.set({"array.slicing.split_large_chunks": False}) # False gives less tasks
    ds = ds.convert_calendar('365_day')   
    
    return ds

#%% ----------------------------------------------------------------   
# read netcdf
def open_annual_array(
    f,
    yi,
): 

    ds = xr.open_dataset(
        f,
        decode_times=False
    )

    t = xr.cftime_range(
        start=yi, 
        periods=ds.sizes['time'], 
        freq='YS', 
        calendar='standard',
    )

    ds['time'] = t # set time
    ds = ds.rename({'tasmax':'tasmax_{}'.format(f.split('_')[0].split('/')[-1])})
    
    return ds

#%% ----------------------------------------------------------------   
# open PIC or obs (ids is array of string identifiers; gcms or obs types)
def collect_arrays(
    ids,
    ddir,
): 
    # native data at daily res
    os.chdir(ddir)
    
    d_daily_data = {}
    
    for i in ids:
        
        files = glob.glob('./{}/*tasmax_*.nc4'.format(i))
        files.sort()
        d_daily_data[i] = []
        
        for f in files:
            
            yi = f.split('_')[-2]
            yf = f.split('_')[-1].split('.')[0]

            if len(yf) > 4: # exception here for PIC YYYYMMDD-YYYYMMDD format

                yi = yf[:4]
            
            if int(yi) <= 2091: # limiting data to 2099
                
                d_daily_data[i].append(open_daily_array(f,yi))

        d_daily_data[i] = xr.concat(d_daily_data[i],dim='time')
        
    ds_daily = xr.merge([d_daily_data[i].rename({'tasmax':'tasmax_{}'.format(i)}) for i in ids])
        
    # preprocessed PIC annual maximums for quantiles
    os.chdir(cdir)
    
    d_ann_data = {}
    yi='1661'
    
    for i in ids:
        
        file = './data/isimip/{}_mergetime_yearmax_1661_2099.nc'.format(i)
        d_ann_data[i] = open_annual_array(file,yi)
        
    ds_ann = xr.merge([d_ann_data[i].drop('time_bnds') for i in ids])
    
    
    return ds_daily,ds_ann

#%% ----------------------------------------------------------------   
# process PIC stats
def hwmid_qntls(
    ds_daily,
    ds_ann,
    ids,
): 
    
    start_time = time.time()
    os.chdir(cdir)
    
    l_qntls = []
    
    for i in ids:
    
        # compute if pickles don't already exist
        if not os.path.isfile('./data/pickles/qntls_{}.pkl'.format(i)):
            
            # annual max from CDO
            da_max = ds_ann['tasmax_{}'.format(i)]

            # empty dataset for quantiles            
            ds_qntls = xr.Dataset(
                coords={
                    'lat': ('lat',ds_ann.lat.data),
                    'lon': ('lon',ds_ann.lon.data),
                },
            )            
            
            # 75th qntl of annual max
            ds_qntls['75_{}'.format(i)] = da_max.quantile(
                q=0.75,
                dim='time',
                method='inverted_cdf',
            )
            
            # 25th qntl of annual max
            ds_qntls['25_{}'.format(i)] = da_max.quantile(
                q=0.25,
                dim='time',
                method='inverted_cdf',
            )
            
            # run window stats on each calendar day for 90th qntl        
            # place holder array for 90% qntl in 31 day windows per day (each calender day will be filled with this qntl)
            ds_qntls['90_{}'.format(i)] = xr.ones_like(ds_daily['tasmax_{}'.format(i)]).isel(time=slice(0,365)).groupby('time.dayofyear').mean('time').load()             
            
            for d in np.arange(1,366):
                
                di = d-15 # window start
                df = d+15 # window end
                w = np.arange(di,df+1) # window in calendar integers
                
                if np.any(w<0): # incase negative values, take calendar days from last year
                    
                    w = np.where(
                        w>0,
                        w,
                        365+w,
                    )
                    
                elif np.any(w>365): # incase values over 365, take calendar days from next year
                    
                    w = np.where(
                        w<=365,
                        w,
                        w-365,
                    )                
                
                # 90th qntl
                ds_qntls['90_{}'.format(i)].loc[{'dayofyear':d}] = xr.map_blocks(
                    multiyr_window_pctl,
                    ds_daily['tasmax_{}'.format(i)],
                    args=[w,d],
                    template=ds_qntls['90_{}'.format(i)].sel(dayofyear=d).chunk({'lat':lat_chunk,'lon':lon_chunk,}),
                )
                
            l_qntls.append(ds_qntls)
            
            # save     
            with open('./data/pickles/qntls_{}.pkl'.format(i), 'wb') as f:
                
                pk.dump(ds_qntls,f)    
        
        # read in pre-existing pickles
        else:
            
            with open('./data/pickles/qntls_{}.pkl'.format(i),'rb') as f:
                ds_qntls = pk.load(f)
                
            l_qntls.append(ds_qntls)
                    
    print("--- {} minutes for computing quantiles---".format(
        np.floor((time.time() - start_time) / 60),
        i
    )
        )     
    
    if len(l_qntls) > 1:
        
        ds_qntls = xr.merge(
            [ds for ds in l_qntls],
        )
        
    return ds_qntls

#%% ----------------------------------------------------------------   
# hot period
def hot_period(
    ds_daily,
    ds_qntls,
    ids,
):
    
    for i in ids:
        
        # make 90% comparable by repeating val across years of da_t and mimicking time dim (
        # maybe don't have to do this because lat/lon/dayofyear c broadcast.. squeeze instead?)
        da_90_cmp = xr.concat(
            [
                ds_qntls['90_{}'.format(i)].assign_coords({'dayofyear':pd.date_range(
                        str(y),
                        periods=ds_qntls['90_{}'.format(i)].sizes['dayofyear'],
                        freq='D'
                    )}).rename({'dayofyear':'time'}) for y in np.unique(ds_daily['tasmax_{}'.format(i)]['time.year'].values)
            ],
            dim='time',
        ).chunk({'lat':lat_chunk,'lon':lon_chunk})
        
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


        
                
