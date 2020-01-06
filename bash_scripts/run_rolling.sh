# This script runs all 4 types of rolling and displays the memory usage of each in the end.
# This should be run from the output directory
maxmem=5GB
nthreads=5

declare -a exps=("xclim" "xrdefault" "xrnocounts" "xrmodified")

# Generating test data
python ../scripts/bench_rolling.py gendata

for exp in "${exps[@]}"
do
  mprof run -C -o "$exp.dat" ../scripts/bench_rolling.py -lc -N $nthreads -m $maxmem $exp
done

mprof plot *.dat



