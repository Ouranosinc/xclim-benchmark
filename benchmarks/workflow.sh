#!/bin/bash
script_dir=$(dirname "$(readlink -f "$0")")
script_args=$1
# loop on all testfiles
files=`find . -type f -name "test_*.py" | sed 's/\.\///'`
for f in $files; do
	#name=`echo $f | sed 's/\.py$//' | sed 's/\//__/g'`
	name=`echo $f | sed 's/\.py$//'`
	mkdir $name
	filename=`python3 conftest.py ${script_args}` 
	pytest_args="-n0 --dist no  ${f} --benchmark-save ${filename} --benchmark-storage ${script_dir}/output/${name}"
	pytest $pytest_args $script_args
done