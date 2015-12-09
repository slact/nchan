#!/bin/bash
par=$1
if [[ -z "$par" ]]; then
  par=5
  echo "No parallel count given. Assuming $par."
fi
if ! [[ $par =~ ^[0-9]+$ ]]; then
  echo "Parallel count isn't a number." > /dev/stderr
  exit 1
fi

for ((i = 0; i < $par; i++)); do

    echo ./bench.rb ${@:2} 
    ./bench.rb ${@:2} &
done

jobs=$(jobs -p)
#echo "jobs are $jobs"
killjobs() {
  for job_pid in $jobs; do
    kill $job_pid
  done
}

trap killjobs SIGINT
wait $jobs
