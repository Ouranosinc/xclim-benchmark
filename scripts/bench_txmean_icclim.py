# Imports
import perf
import icclim

def func():
    files = []
    files.append('/media/proy/HDD500GB/DATA/CMIP5/tasmax_day_MIROC5_historical_r1i1p1_19500101-19591231.nc')
    # files.append('/media/proy/HDD500GB/DATA/CMIP5/tasmax_day_MIROC5_historical_r1i1p1_19600101-19691231.nc')

    icclim.indice(files, 'tasmax', indice_name='TG', out_file='temp.nc')


runner = perf.Runner()
runner.bench_func('ICCLIM_TG', func)


