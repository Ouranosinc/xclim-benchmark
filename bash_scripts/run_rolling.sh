# This script runs 3 types of rolling and displays the memory usage of each in the end.
# This should be run from the output directory
maxmem=40GB
nthreads=32

declare -a exps=("xclim" "xrdefault" "xrnocounts")

# Generating test data
python ../scripts/bench_rolling.py gendata -n 350 200 150 120

for exp in "${exps[@]}"
do
  mprof run -C ../scripts/bench_rolling.py -cls -N $nthreads -m $maxmem $exp
done

python ../scripts/bench_rolling.py plot
