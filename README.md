# pytest implementation 

## Description

Benchmarks for `xclim` using the [`pytest-benchmark`](https://pypi.org/project/pytest-benchmark/).

# Usage 

## Running benchmarks

`workflow.sh` loops over all `test_*.py` files in `benchmarks` directory and its subdirectories. 

```bash
script_args="--with-client True --chunk-size location=1 --nthreads 4 --max-mem 2GB --nworkers 3"
bash workflow.sh $script_args
```
A single benchmark on a `file` grossly in 
```bash
pytest_args="-n0 --dist no ${file}.py"
pytest $pytest_args $script_args
```

## Output

Every `test_*.py` has a corresponding directory in `output` where the benchmark are stored. 
# Old pyperf implementation

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
