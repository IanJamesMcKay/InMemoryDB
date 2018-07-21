#!/bin/bash

if [ $# -lt 3 ]
then
	echo Runs a benchmark for every commit in a given range
	echo Call the script from your build directory \(i.e., ../scripts/$0\)
	echo Arguments: first_commit_id last_commit_id binary [benchmark_arguments]
	echo binary is, for example, hyriseBenchmarkTPCH
	exit 1
fi

start_hash=$1
end_hash=$2
benchmark=$3
shift; shift; shift
benchmark_arguments=$@

if [[ $(git status --untracked-files=no --porcelain) ]]
then
	echo Cowardly refusing to execute on a dirty workspace
	exit 1
fi

commit_list=$(git rev-list --ancestry-path ${start_hash}^..${end_hash})

[[ -z "$commit_list" ]] && echo No connection between these commits found && exit

commit_list=$(echo $commit_list | awk '{for (i=NF; i>1; i--) printf("%s ",$i); printf("%s\n",$1)}')  ## revert list

for commit in $commit_list
do
	echo ======================================================
	git rev-list --format=%B --max-count=1 $commit | head -n 2
	echo

	git checkout $commit >/dev/null 2>/dev/null

	cores=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || sysctl -n hw.ncpu)
	make $3 -j $((cores / 2)) >/dev/null

	$benchmark $benchmark_arguments > ${commit}.json

	[[ -z "$previous_commit" ]] && ./scripts/compare_benchmarks.py ${previous_commit}.json ${commit}.json

	previous_commit=commit
done