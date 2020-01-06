# Rolling benchmarks
# Comparing xarray's defaults with custom implmentations
# Also comparing different usages of dask.
import argparse
import xclim as xc
import numpy as np
import xarray as xr
import datetime as dt
import dask.array as dsk
from distributed import Client
testfile = 'testdata_t{}.nc'
outfile = 'testout_{}{}{}.nc'


def read_mprofile(filename):
    times = []
    mem = []
    name = filename.split('.')[0]
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('CMDLINE'):
                name = line.strip().split()[-1]
            elif line.startswith('MEM'):
                _, m, t = line.strip().split()
                times.append(dt.datetime.fromtimestamp(t))
                mem.append(float(m))
    times = [(t - t[0]).total_seconds() / 60 for t in times]
    return name, times, mem


def xclim_custom(data, dim, window, func):
    return xc.utils._rolling(data, dim=dim, window=window, mode=func)


def xr_default(data, dim, window, func, lazy=False):
    return getattr(data.rolling(dim={dim: window}), func)(allow_lazy=lazy)


def xr_nocounts(data, dim, window, func, lazy=False):
    func = getattr(dsk, func)
    return data.rolling(dim={dim: window}).construct('window_dim').reduce(func, dim='window_dim', allow_lazy=lazy)


def xr_modified(data, dim, window, func, lazy=False):

    func = getattr(dsk, func)
    out = data.rolling(dim={dim: window}).construct('window_dim').reduce(func, dim='window_dim', allow_lazy=lazy)
    counts = (
        data.notnull()
        .rolling(dim={dim: window})
        .construct('window_dim', fill_value=False)
        .sum(dim='window_dim', skipna=False, allow_lazy=lazy)
    )
    return out.where(counts == window)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Profile memory for rolling functions')
    parser.add_argument('exp', type=str, help='which exp to run')
    parser.add_argument('-f', '--func', type=str, default='mean', help='which function to run')
    parser.add_argument('-l', '--lazy', action='store_true', help='whether to allow lazy (xr)')
    parser.add_argument('-c', '--with-client', action='store_true', help='whether to use a dask client')
    parser.add_argument('-N', '--nthreads', default=10, type=int, help='When using a dask client, number of threads per worker')
    parser.add_argument('-m', '--max-mem', default='2GB', help='When using a dask client, memory limit')
    parser.add_argument('-i', '--files', default='*.dat', nargs='*', help='Dat files to plot')
    args = parser.parse_args()

    if args.exp == 'gendata':
        for i in range(40):
            print(f'Generating data {i+1:02d}/20')
            data = xr.DataArray(data=np.random.random((500, 100, 100)),
                                dims=('time', 'x', 'y'),
                                coords={'time': np.arange(500 * i, 500 * (i + 1)),
                                        'x': np.arange(100), 'y': np.arange(100)},
                                name='data')
            data.to_netcdf(testfile.format(f'{i:02d}'))
    elif args.exp == 'plot'
        import matplotlib.pyplot as plt
        if not isinstance(args.files, list):
            if '*' in args.files:
                files = glob.glob(args.files)
            else:
                files = [args.files]
        else:
            files = args.files

        fig, ax = plt.subplots(figsize=(10, 5))
        for file in files:
            name, times, mem = read_mprofile(file)
            ax.plot(times, mem, label=name)


    else:
        if args.with_client:
            c = Client(n_workers=1, threads_per_worker=args.nthreads, memory_limit=args.max_mem)

        data = xr.open_mfdataset(testfile.format('*'), combine='by_coords', chunks={})

        print(f'Running rolling with exp: {args.exp}')
        if args.exp == 'xclim':
            out = xclim_custom(data.data, 'time', 5, args.func)
        elif args.exp == 'xrdefault':
            out = xr_default(data.data, 'time', 5, args.func, lazy=args.lazy)
        elif args.exp == 'xrnocounts':
            out = xr_nocounts(data.data, 'time', 5, args.func, lazy=args.lazy)
        elif args.exp == 'xrmodified':
            out = xr_modified(data.data, 'time', 5, args.func, lazy=args.lazy)

        print('Writing to file')
        out.to_netcdf(outfile.format(args.exp,
                                     '_lazy' if args.lazy else '',
                                     f'_dskcli_{args.nthreads:02d}_{args.max_mem}' if args.with_client else ''))
        out.close()

        if args.with_client:
            c.close()
