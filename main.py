#!/usr/bin/env python3
# ---------------------------------------------------------------
# Main script to calculate HWMId (PIC thresholds and ISIMIP3a input conversions)
#
# FILL IN GITHUB
# ----------------------------------------------------------------

#%% ----------------------------------------------------------------
# Summary and notes

# Data types are defined in the variable names starting with:  
#     df_     : DataFrame    (pandas)
#     gdf_    : GeoDataFrame (geopandas)
#     da_     : DataArray    (xarray)
#     d_      : dictionary  
#     sf_     : shapefile
#     ...dir  : directory

#               
#%% ----------------------------------------------------------------
# import and path
# ----------------------------------------------------------------
if __name__ == '__main__':
    
    import xarray as xr
    import pickle as pk
    import time
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    import matplotlib as mpl
    import mapclassify as mc
    from copy import deepcopy as cp
    import os
    import matplotlib.pyplot as plt
    import pandas as pd
    import dask

    import multiprocessing, os
    def doubler(x): return x*2
    ncore = len(os.sched_getaffinity(0))
    print('allocated cores:', ncore)
    with multiprocessing.Pool(processes=ncore) as p:
        print(p.map(doubler, range(20)))
            
    from dask.distributed import Client, LocalCluster
    cluster = LocalCluster(dashboard_address='12345', n_workers=8, threads_per_worker=3, local_directory='/scratch/brussel/vo/000/bvo00012/vsc10116/hwmid')
    client = Client(cluster)
    client
    from pprint import pprint
    pprint(cluster.worker_spec)
    print(client)

    #%% ----------------------------------------------------------------
    # flags
    # ----------------------------------------------------------------

    global flags

    flags = {}
    flags['obs'] = 0           # 0: do not process ISIMIP3a obs tasmax runs (i.e. load runs pickle)
                            # 1: process ISIMIP runs (i.e. produce and save runs as pickle)
    flags['pic'] = 0           # 0: do not process ISIMIP2b pic tasmax runs (i.e. load runs pickle)
                            # 1: process ISIMIP runs (i.e. produce and save runs as pickle)


    #%% ----------------------------------------------------------------
    # initialize
    # ----------------------------------------------------------------
    import settings
    gcms, obs_types, pdir, cdir, lat_chunk, lon_chunk, time_chunk = settings.init()    

    #%% ----------------------------------------------------------------
    # read in daily pic and annual max pic
    # ----------------------------------------------------------------

    from load_manip import *

    ds_pic_daily,ds_pic_ann_max = collect_arrays(
        gcms,
        pdir,
    )

    #%% ----------------------------------------------------------------
    # retrieve 25th, 75th and 90th quantiles of pic
    # ----------------------------------------------------------------

    ds_pic_qntls = hwmid_qntls(
        ds_pic_daily,
        ds_pic_ann_max,
        gcms,
    )



























