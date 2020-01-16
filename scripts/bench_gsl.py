# Growing season length benchmarks
# Comparing different implementations
import glob
import argparse
import numpy as np
import xclim as xc
import pandas as pd
import xarray as xr
import datetime as dt
from xclim import run_length as rl
from distributed import Client
testfile = 'testdata_i{i}.nc'
outfile = 'testout_{}.nc'
window = 6
thresh = 5


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


def exp_smallchange(tas):
    c = ((tas > thresh) * 1).rolling(time=window).sum(allow_lazy=True, skipna=False)

    def compute_gsl(c):
        nt = c.time.size
        i = xr.DataArray(np.arange(nt), dims="time")
        ind = xr.broadcast(i, c)[0].chunk(c.chunks)
        i1 = ind.where(c == window).min(dim="time")
        i2 = ind.where((c == 0) & (c.time.dt.month >= 7)).min(dim='time')
        d = (i2 - i1)
        d = d.where(d > 0)
        return d

    return c.resample(time='YS').apply(compute_gsl)


def exp_firstrun(tas):
    def func(group):
        deb = rl.first_run(group.where(group.time.dt.month < 7) > thresh, window, 'time')
        fin = rl.first_run(group.where(group.time.dt.month >= 7) < thresh, window, 'time')
        return fin - deb

    return tas.resample(time='YS').apply(func)


def exp_firstrunnocheck(tas):
    def func(group):
        deb = rl.first_run(group > thresh, window, 'time')
        fin = rl.first_run(group.where(group.time.dt.month >= 7) < thresh, window, 'time')
        return fin - deb

    return tas.resample(time='YS').apply(func)


def exp_firstruncheck(tas):
    def func(group):
        sl = rl.first_run(group.where(group.time.dt.month >= 7) < thresh, window, 'time') - rl.first_run(group > thresh, window, 'time')
        return sl.where(sl > 0)

    return tas.resample(time='YS').apply(func)


def exp_firstrunisel(tas):
    def func(group):
        fin = rl.first_run(group.where(group.time.dt.month >= 7) < thresh, window, 'time')
        deb = rl.first_run(group.where(group.time.dt.dayofyear < fin) > thresh, window, 'time')
        return fin - deb

    return tas.resample(time='YS').apply(func)


def exp_xcdef(tas, window=6, thresh=5):
    return xc.indices.growing_season_length(tas)


all_exps = {name.split('_')[1]: func for name, func in globals().items() if name.startswith('exp')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Profile memory for rolling functions')
    parser.add_argument('-c', '--with-client', action='store_true', help='whether to use a dask client')
    parser.add_argument('-N', '--nthreads', default=10, type=int, help='When using a dask client, number of threads per worker')
    parser.add_argument('-m', '--max-mem', default='2GB', help='When using a dask client, memory limit')
    parser.add_argument('exp', type=str, help='which exp to run')
    parser.add_argument('-i', '--files', default='*.dat', nargs='*', help='Dat files to plot')
    parser.add_argument('-n', '--chunk-size', default=[3, 100, 100, 1], nargs='*', help='Size of the random data to generate. 1, 2, 3 or 4 values for t (n years), x, y and nchunks/yr. Data is daily.')
    args = parser.parse_args()

    if args.exp == 'gendata':
        if isinstance(args.chunk_size, list):
            if len(args.chunk_size) == 2:
                Nt, Nc = map(int, args.chunk_size)
                Ny = Nx = Nt
            elif len(args.chunk_size) == 3:
                Nt, Nx, Nc = map(int, args.chunk_size)
                Ny = Nt
            else:
                Nt, Nx, Ny, Nc = map(int, args.chunk_size)
        else:
            Nt = Nx = Ny = int(args.chunk_size)
            Nc = 10
        times = pd.date_range('2000-01-01', f'20{Nt:02d}-12-31', freq='D')
        times = xr.DataArray(times, coords={'time': times}, dims=('time',), name='time')
        chunkSize = 365 // Nc
        for i in range(Nt):
            data = np.random.random((times.sel(time=f'20{i:02d}').size, Nx, Ny)) - 20 * np.cos(2 * np.pi * times.sel(time=f'20{i:02d}').dt.dayofyear.values / 366)[:, np.newaxis, np.newaxis]
            for c in range(Nc):
                print(f'Generating data {i * Nc + c + 1:02d}/{Nc * Nt}')
                data = xr.DataArray(data=data[slice(c * chunkSize, (c + 1) * chunkSize if c + 1 < Nc else None)],
                                    dims=('time', 'x', 'y'),
                                    coords={'time': times.sel(time=f'20{i:02d}').isel(time=slice(c * chunkSize, (c + 1) * chunkSize if c + 1 < Nc else None)),
                                            'x': np.arange(Nx), 'y': np.arange(Ny)},
                                    name='data',
                                    attrs={'units': 'degC'})
                data[chunkSize // 2, 0, 0] = np.nan
                data.to_netcdf(testfile.format(i=f'{i:02d}'))

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
        colors = {exp: col for exp, col in zip(all_exps.keys(),
                                               plt.matplotlib.rcParams['axes.prop_cycle'].by_key()['color'])}
        fig, ax = plt.subplots(figsize=(10, 5))
        for file in files:
            name, times, mem = read_mprofile(file)
            ax.plot(times, mem, label=name, color=colors[name])
        ax.legend()
        ax.set_xlabel('Computation time [s]')
        ax.set_ylabel('Memory usage [MiB]')
        ax.set_title('Memory usage of different percentile calculations')
        plt.show()

    else:
        if args.with_client:
            c = Client(n_workers=1, threads_per_worker=args.nthreads, memory_limit=args.max_mem)

        # num_real = len(glob.glob(testfile.format(r='*', i=0)))

        ds = xr.open_mfdataset(testfile.format(i='*'))
        ds.data.attrs.update(units='degC')
        print(f'Running rolling with exp: {args.exp}')
        ds_out = all_exps[args.exp](ds.data)

        print('Writing to file')

        r = ds_out.to_netcdf(outfile.format(args.exp), compute=False)
        r.compute()
        ds_out.close()

        if args.with_client:
            c.close()
