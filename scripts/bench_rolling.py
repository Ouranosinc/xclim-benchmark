# Rolling benchmarks
# Comparing xarray's defaults with custom implmentations
# Also comparing different usages of dask.
import glob
import argparse
import xclim as xc
import numpy as np
import xarray as xr
import datetime as dt
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
                times.append(dt.datetime.fromtimestamp(float(t)))
                mem.append(float(m))
    times = [(t - times[0]).total_seconds() for t in times]
    return name, times, mem


def xclim_custom(data, dim, window, func):
    return xc.utils._rolling(data, dim=dim, window=window, mode=func)


def xr_default(data, dim, window, func, lazy=False, skipna=None):
    return getattr(data.rolling(dim={dim: window}), func)(allow_lazy=lazy, skipna=skipna)


def xr_nocounts(data, dim, window, func, lazy=False, skipna=None):
    return data.rolling(dim={dim: window}).construct('window_dim').reduce(func, dim='window_dim', allow_lazy=lazy, skipna=skipna)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Profile memory for rolling functions')
    parser.add_argument('-f', '--func', type=str, default='mean', help='which function to run')
    parser.add_argument('-l', '--lazy', action='store_true', help='whether to allow lazy (xr)')
    parser.add_argument('-c', '--with-client', action='store_true', help='whether to use a dask client')
    parser.add_argument('-N', '--nthreads', default=10, type=int, help='When using a dask client, number of threads per worker')
    parser.add_argument('-m', '--max-mem', default='2GB', help='When using a dask client, memory limit')
    parser.add_argument('-s', '--skipna', action='store_true', help='If specified, passes skipna=True')
    parser.add_argument('exp', type=str, help='which exp to run')
    parser.add_argument('-i', '--files', default='*.dat', nargs='*', help='Dat files to plot')
    parser.add_argument('-n', '--chunk-size', default=[500, 100, 100, 20], nargs='*', help='Size of the random data to generate. 1, 2, 3 or 4 values for t, x, y and nchunks.')
    args = parser.parse_args()

    if args.exp == 'gendata':
        if isinstance(args.chunk_size, list):
            if len(args.chunk_size) == 2:
                Nt, Nc = map(int, args.chunk_size)
                Ny = Nx = Nt
            elif len(args.chunk_size) == 3:
                Nt, Nx, Nc = map(int, args.chunk_size)
                Ny = Nx
            else:
                Nt, Nx, Ny, Nc = map(int, args.chunk_size)
        else:
            Nt = Nx = Ny = int(args.chunk_size)
            Nc = 20

        for i in range(Nc):
            print(f'Generating data {i+1:02d}/{Nc}')
            data = xr.DataArray(data=np.random.random((Nt, Nx, Ny)),
                                dims=('time', 'x', 'y'),
                                coords={'time': np.arange(Nt * i, Nt * (i + 1)),
                                        'x': np.arange(Nx), 'y': np.arange(Ny)},
                                name='data')
            data.to_netcdf(testfile.format(f'{i:02d}'))

    elif args.exp == 'plot':
        import matplotlib.pyplot as plt
        try:
            plt.style.use('dark_background')
        except OSError:
            pass
        if not isinstance(args.files, list):
            if '*' in args.files:
                files = glob.glob(args.files)
            else:
                files = [args.files]
        else:
            files = args.files
        colors = {exp: col for exp, col in zip(['xclim', 'xrpure', 'xrdefault', 'xrnocounts'],
                                               plt.matplotlib.rcParams['axes.prop_cycle'].by_key()['color'])}
        fig, ax = plt.subplots(figsize=(10, 5))
        for file in files:
            name, times, mem = read_mprofile(file)
            ax.plot(times, mem, label=name, color=colors[name])
        ax.legend()
        ax.set_xlabel('Computation time [s]')
        ax.set_ylabel('Memory usage [MiB]')
        ax.set_title('Memory usage of different rolling methods')
        plt.show()

    else:
        if args.with_client:
            c = Client(n_workers=1, threads_per_worker=args.nthreads, memory_limit=args.max_mem)

        data = xr.open_mfdataset(testfile.format('*'), combine='by_coords', chunks={})

        func = getattr(xr.core.duck_array_ops, args.func)
        print(f'Running rolling with exp: {args.exp}')
        if args.exp == 'xclim':
            out = xclim_custom(data.data, 'time', 5, args.func)
        elif args.exp == 'xrpure':
            out = xr_default(data.data, 'time', 5, args.func)
        elif args.exp == 'xrdefault':
            out = xr_default(data.data, 'time', 5, args.func, lazy=args.lazy, skipna=args.skipna)
        elif args.exp == 'xrnocounts':
            out = xr_nocounts(data.data, 'time', 5, func, lazy=args.lazy, skipna=args.skipna)

        print('Writing to file')
        r = out.to_netcdf(outfile.format(args.exp,
                                         '_lazy' if args.lazy else '',
                                         f'_dskcli_{args.nthreads:02d}_{args.max_mem}' if args.with_client else ''),
                          compute=False)
        r.compute()
        out.close()

        if args.with_client:
            c.close()
