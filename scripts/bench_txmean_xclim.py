
import perf
import time
import xclim as xc
import xarray as xr

# # Import NRCAN datasets
# files = []
# files.append('/media/proy/HDD500GB/DATA/CMIP5/tasmax_day_MIROC5_historical_r1i1p1_19500101-19591231.nc')
# files.append('/media/proy/HDD500GB/DATA/CMIP5/tasmax_day_MIROC5_historical_r1i1p1_19600101-19691231.nc')
#
#
# dataset = xr.open_mfdataset(files)
# tasmax = dataset['tasmax']
#
# TG = xc.indices.tx_mean(tasmax)
#
# TG.to_netcdf('xclim.nc')

#!/usr/bin/env python3




def func():
    files = '/media/proy/HDD500GB/DATA/CMIP5/tasmax_day_MIROC5_historical_r1i1p1_19500101-19591231.nc'
    files = []
    files.append('/media/proy/HDD500GB/DATA/Subsets/NRCAN/nrcan_canada_daily_pr_1961_subset.nc')
    files.append('/media/proy/HDD500GB/DATA/Subsets/NRCAN/nrcan_canada_daily_pr_1962_subset.nc')
    # files = "/media/proy/HDD500GB/DATA/CMIP5/tasmax_day_MIROC5_historical_r1i1p1_19500101-19591231.nc"
    # files = []
    # files.append('/media/proy/HDD500GB/DATA/CMIP5/tasmax_day_MIROC5_historical_r1i1p1_19500101-19591231.nc')
    # files.append('/media/proy/HDD500GB/DATA/CMIP5/tasmax_day_MIROC5_historical_r1i1p1_19600101-19691231.nc')

    dataset = xr.open_dataset(files)
    # dataset = xr.open_mfdataset(files)

    tasmax = dataset['tasmax'][:, i, j]

    TG = xc.indices.tx_mean(tasmax)

    TG.to_netcdf('xclim.nc')



runner = perf.Runner()
runner.bench_func('XCLIM_tx_mean', func)






# numiter = 50
# repeat = 2
#
# t_xclim = min(Timer("xc.indices.tx_mean(tasmax)", setup=setup_xclim).repeat(repeat, numiter)) / numiter
#
# t_icclim = min(Timer("icclim.indice(files, 'tasmax', indice_name='TG', out_file='temp.nc')", setup=setup_icclim).repeat(repeat, numiter)) / numiter
#
# print("XCLIM : " + str(t_xclim) + " sec")
