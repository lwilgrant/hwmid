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

# TODO


#               
#%% ----------------------------------------------------------------
# import and path
# ----------------------------------------------------------------

import xarray as xr
import pickle as pk
import time
from mpl_toolkits.axes_grid1 import make_axes_locatable
import matplotlib as mpl
import mapclassify as mc
from copy import deepcopy as cp
import os
import matplotlib.pyplot as plt


#%% ----------------------------------------------------------------
# flags
# ----------------------------------------------------------------

# extreme event
global flags

flags = {}
flags['obs'] = 0           # 0: do not process ISIMIP3a obs tasmax runs (i.e. load runs pickle)
                           # 1: process ISIMIP runs (i.e. produce and save runs as pickle)
flags['pic'] = 0           # 0: do not process ISIMIP2b pic tasmax runs (i.e. load runs pickle)
                           # 1: process ISIMIP runs (i.e. produce and save runs as pickle)


#%% ----------------------------------------------------------------
# initialize
# ----------------------------------------------------------------
from settings import *

# set global variables
init()


#%% ----------------------------------------------------------------
# load and manipulate ISIMIP data
# ----------------------------------------------------------------

from load_manip import *
 
ds_pic = collect_arrays(
    gcms,
    pdir
)

# %%
