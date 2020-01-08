# Ensemble percentile benchmarks
# Comparing different implementation
import glob
import argparse
from xclim import ensembles as xcens
import numpy as np
import xarray as xr
import datetime as dt
from distributed import Client
testfile = 'testdata_r{r}_i{i}.nc'
outfile = 'testout_{}.nc'


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


def exp_xcdef(ds, percentiles):
    return xcens.ensemble_percentiles(ds, percentiles)


def _calc_perc(arr, p=50):
    nan_count = np.isnan(arr).sum(axis=-1)
    out = np.percentile(arr, p, axis=-1)
    nans = (nan_count > 0) & (nan_count < arr.shape[-1])
    if np.any(nans):
        arr1 = arr.reshape(int(arr.size / arr.shape[-1]), arr.shape[-1])
        # only use nanpercentile where we need it (slow performace compared to standard) :
        nan_index = np.where(nans)
        t = np.ravel_multi_index(nan_index, nan_count.shape)
        out[np.unravel_index(t, nan_count.shape)] = np.nanpercentile(
            arr1[t, :], p, axis=-1
        )

    return out


def _calc_nanperc(arr, p=50):
    return np.nanpercentile(arr, p, axis=-1)


def ensemble_percs(ds, ps):
    ds_out = ds.drop_vars(ds.data_vars)
    for v in ds.data_vars:
        for p in ps:
            if len(ds.chunks.get('realization', [])) > 1:
                var = ds[v].chunk({'realization': -1})
            else:
                var = ds[v]
            perc = xr.apply_ufunc(
                _calc_perc,
                var,
                input_core_dims=[['realization']],
                output_core_dims=[[]],
                keep_attrs=True,
                kwargs=dict(p=p),
                dask='parallelized',
                output_dtypes=[ds[v].dtype]
            )

            perc.name = v + f'_{p:02d}'
            perc.attrs.update(description=f'{p:02d}th percentile of ensemble')
            ds_out[perc.name] = perc
    return ds_out


def ensemble_nanpercs(ds, ps):
    ds_out = ds.drop_vars(ds.data_vars)
    for v in ds.data_vars:
        for p in ps:
            if len(ds.chunks.get('realization', [])) > 1:
                var = ds[v].chunk({'realization': -1})
            else:
                var = ds[v]
            perc = xr.apply_ufunc(
                _calc_nanperc,
                var,
                input_core_dims=[['realization']],
                output_core_dims=[[]],
                keep_attrs=True,
                kwargs=dict(p=p),
                dask='parallelized',
                output_dtypes=[ds[v].dtype]
            )

            perc.name = v + f'_{p:02d}'
            perc.attrs.update(description=f'{p:02d}th percentile of ensemble')
            ds_out[perc.name] = perc
    return ds_out


def ensemble_percs_rechunk(ds, ps):
    ds_out = ds.drop_vars(ds.data_vars)
    for v in ds.data_vars:
        for p in ps:
            if len(ds.chunks.get('realization', [])) > 1:
                var = ds[v].chunk({'realization': -1, 'time': len(ds.chunks['time']) * len(ds.chunks['realization'])})
            else:
                var = ds[v]
            perc = xr.apply_ufunc(
                _calc_perc,
                var,
                input_core_dims=[['realization']],
                output_core_dims=[[]],
                keep_attrs=True,
                kwargs=dict(p=p),
                dask='parallelized',
                output_dtypes=[ds[v].dtype]
            )

            perc.name = v + f'_{p:02d}'
            perc.attrs.update(description=f'{p:02d}th percentile of ensemble')
            ds_out[perc.name] = perc
    return ds_out


def exp_xrapply(ds, percentiles):
    return ensemble_percs(ds, percentiles)


def exp_xrnanapply(ds, percentiles):
    return ensemble_nanpercs(ds, percentiles)


def exp_xrapplyrechunk(ds, percentiles):
    return ensemble_percs_rechunk(ds, percentiles)


def exp_xrrednan(ds, percentiles):
    ds_out = ds.drop_vars(ds.data_vars)
    for v in ds.data_vars:
        for p in percentiles:
            perc = ds[v].reduce(lambda d, **kws: np.nanpercentile(d, p, **kws),
                                dim='realization',
                                keep_attrs=True)
            perc.name = v + f'_{p:02d}'
            perc.attrs.update(description=f'{p:02d}th percentile of ensemble')
            ds_out[perc.name] = perc
    return ds_out


def exp_xrquantile(ds, percentiles):
    return ds.quantile([p / 100 for p in percentiles], dim='realization', keep_attrs=True)


all_exps = {name.split('_')[1]: func for name, func in globals().items() if name.startswith('exp')}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Profile memory for rolling functions')
    parser.add_argument('-c', '--with-client', action='store_true', help='whether to use a dask client')
    parser.add_argument('-N', '--nthreads', default=10, type=int, help='When using a dask client, number of threads per worker')
    parser.add_argument('-m', '--max-mem', default='2GB', help='When using a dask client, memory limit')
    parser.add_argument('exp', type=str, help='which exp to run')
    parser.add_argument('-i', '--files', default='*.dat', nargs='*', help='Dat files to plot')
    parser.add_argument('-n', '--chunk-size', default=[300, 100, 100, 10, 10], nargs='*', help='Size of the random data to generate. 1, 2, 3, 4 or 5 values for t, x, y, nchunks and nensemble.')
    args = parser.parse_args()

    if args.exp == 'gendata':
        if isinstance(args.chunk_size, list):
            if len(args.chunk_size) == 2:
                Nt, Nc = map(int, args.chunk_size)
                Ny = Nx = Nt
                Nr = 10
            elif len(args.chunk_size) == 3:
                Nt, Nc = map(int, args.chunk_size)
                Ny = Nx = Nt
            elif len(args.chunk_size) == 4:
                Nt, Nx, Nc, Nr = map(int, args.chunk_size)
                Ny = Nx
            else:
                Nt, Nx, Ny, Nc, Nr = map(int, args.chunk_size)
        else:
            Nt = Nx = Ny = int(args.chunk_size)
            Nc = 20

        times = np.arange(Nc * Nt)
        for r in range(Nr):
            for i in range(Nc):
                print(f'Generating data {r * Nc + i + 1:02d}/{Nc * Nr}')
                data = xr.DataArray(data=np.random.random((Nt, Nx, Ny)),
                                    dims=('time', 'x', 'y'),
                                    coords={'time': times[Nt * i:Nt * (i + 1)],
                                            'x': np.arange(Nx), 'y': np.arange(Ny)},
                                    name='data')
                data[0, 0, 0] = np.nan
                data.time.attrs.update(axis='T', units='days since 2000-01-01 00:00:00',
                                       long_name='time', calendar='noleap')
                data.to_netcdf(testfile.format(r=r, i=i))

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

        num_real = len(glob.glob(testfile.format(r='*', i=0)))
        ds = xcens.create_ensemble([glob.glob(testfile.format(r=r, i='*')) for r in range(num_real)],
                                   mf_flag=True,
                                   combine='by_coords')

        print(f'Running rolling with exp: {args.exp}')
        ds_out = all_exps[args.exp](ds, [10, 50, 90])

        print('Writing to file')

        r = ds_out.to_netcdf(outfile.format(args.exp), compute=False)
        r.compute()
        ds_out.close()

        if args.with_client:
            c.close()
