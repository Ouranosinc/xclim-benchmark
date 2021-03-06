## Description

Benchmarks for `xclim` using the [`pyperf`](https://pyperf.readthedocs.io/en/latest/) package or the [`memory_profiles`](https://pypi.org/project/memory-profiler/) package.
# Usage
## Running benchmarks

### Single benchmark

To run a `pyperf` single benchmark. In the terminal, the folowing will run the benchmark for xclim and indice `tx_mean`.

`python bench_xclim.py -o ./output/bench_xclim.json`

### Rolling benchmarks

Given the installed xclim version contains `xc.utils._rolling` and `xarray<=0.14.1`, the benchmark can be run with:

```
python ../scripts/bench_rolling.py gendata
python ../scripts/bench_rolling.py -c -l -s xclim
```

See the documentation `python ../scripts/bench_rolling.py --help` for all options.

### All benchmark

To run all benchmarks, launch the bash script.

## Consulting benchmark

To consult a single benchmark. In the terminal:

`python -m pyperf stats bench.json`
