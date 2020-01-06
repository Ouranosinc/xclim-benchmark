## Description

Benchmarks for `xclim` using the [`pyperf`](https://pyperf.readthedocs.io/en/latest/) package.
# Usage
## Running benchmarks

### Single benchmark

To run a single benchmark. In the terminal, the folowing will run the benchmark for xclim and indice `tx_mean`.

`python bench_xclim.py -o ./output/bench_xclim.json`

### All benchmark

To run all benchmarks, launch the bash script.

## Consulting benchmark

To consult a single benchmark. In the terminal:

`python -m pyperf stats bench.json`
