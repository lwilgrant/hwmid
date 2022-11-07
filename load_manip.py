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
# # load ISIMIP model data
# def load_dicts(
#     extremes, 
# ): 
    
#     # loac pickled metadata for isimip and isimip-pic simulations
#     extreme = extremes[0]

#     with open('./data/pickles/isimip_metadata_{}.pkl'.format(extreme), 'rb') as f:
#         d_isimip_meta = pk.load(f)
#     with open('./data/pickles/isimip_pic_metadata_{}.pkl'.format(extreme), 'rb') as f:
#         d_pic_meta = pk.load(f)                

#     return d_isimip_meta,d_pic_meta

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
    
    da['time'] = t
    
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
        data[i] = []
        
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
        
    # # for gcms, add final data var ('all') which is all 4 GCM's pic data concatenated with 0->len(all) time dimension
    # if ids == gcms:
        
    #     ds['all'] = xr.concat(
    #         [ds[gcm].assign_coords({'time':np.arange(i*len(ds[gcm].time),(i+1)*len(ds[gcm].time))}) for i,gcm in enumerate(gcms)],
    #         dim='time',
    #     )
    
    return ds

#%% ----------------------------------------------------------------   
# process PIC stats
def hwmid(
    ds,
): 
    
    for da in ds.data_vars:
        
        ds[da+'_75'] = ds[da].groupby('time.year').max('year').quantile(
            q=0.75,
            dim='year',
            method='inverted_cdf'
        )

        ds[da+'_25'] = ds[da].groupby('time.year').max('year').quantile(
            q=0.25,
            dim='year',
            method='inverted_cdf'
        )        
        
        # ds[da+'_90'] = ds
        
        # run window stats on each day
        for d in np.arange(1,366):
            
            i = d-15
            f = d+15
            w = np.arange(i,f+1)
            
            if np.any(w<0):
                
                w = np.where(
                    w>0,
                    w,
                    365+w,
                )
                
            elif np.any(w>365):
                
                w = np.where(
                    w<=365,
                    w,
                    w-365,
                )                
            
            ds[da+'_90'] = ds[da].sel(time=ds['time.dayofyear'].isin(w)).quantile(
                q=0.9,
                dim='time',
                method='inverted_cdf',
            )
    
    return ds


# %%
