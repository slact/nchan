#!/bin/zsh

for (( n=7000; n<=7005; n++ ))
do
  redis-server ./redis-${n}.conf &
done

echo "all redis-servers in cluster started."

wait
