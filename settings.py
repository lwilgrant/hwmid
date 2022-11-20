#!/usr/bin/env python3
# ----------------------------------------------------------------
# Settings
# These are global variables to be used throughout the whole project
# ----------------------------------------------------------------

import numpy as np

#%% ----------------------------------------------------------------
def init(): 
    
    # GCMs
    global gcms
    gcms = (
        'GFDL-ESM2M',
        'HadGEM2-ES',
        'IPSL-CM5A-LR',
        'MIROC5',
    )
    
    # obs types
    global obs_types
    obs_types = (
        '20CRv3',
        '20CRv3-ERA5',
        '20CRv3-W5E5',
        'GSWP3-W5E5',
    )
    
    # dimension chunk sizes
    global lat_chunk, lon_chunk, time_chunk
    lat_chunk = 90
    lon_chunk = 120
    time_chunk = -1
    
    # data directories
    global pdir,cdir #, odir, cdir
    pdir = '/vscmnt/brussel_pixiu_data/_data_brussel/vo/000/bvo00012/data/dataset/ISIMIP/ISIMIP2b/InputData/GCM_atmosphere/biascorrected/global/piControl' # PIC
    cdir = '/vscmnt/brussel_pixiu_data/_data_brussel/vo/000/bvo00012/vsc10116/lifetime_exposure_isimip/hwmid'
    
    return gcms, obs_types, pdir, cdir, lat_chunk, lon_chunk, time_chunk

# %%
