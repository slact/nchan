#!/bin/zsh

PORTMIN=7000
PORTMAX=7005

slaves=()
node_slaves=()

info=$(redis-cli --raw -p $PORTMIN cluster info)

if ! echo $info | ack 'cluster_state:ok' > /dev/null; then
  echo "cluster not ready" > /dev/stderr
  exit 1
fi    

function is_master {
  resp=$(redis-cli --raw -p $1 eval "return redis.call('info', 'replication'):match('role:master') and 1 or 0" 0)
  if [[ $resp == "1" ]]; then
    return 0
  else
    return 1
  fi
}

for (( n=$PORTMIN; n<=$PORTMAX; n++ )); do
  if ! (is_master $n); then
    slaves+=($n)
    node_slaves[$n]=1
  fi
done

for n in $slaves; do
  echo -n "$n:"
  while ! (is_master $n); do
    echo -n "."
    redis-cli -p $n CLUSTER FAILOVER > /dev/null
    sleep 0.2
  done;
  echo " ok"
done

repeat="repeat"
while [[ $repeat == "repeat" ]]; do
  repeat=""
  for (( n=$PORTMIN; n<=$PORTMAX; n++ )); do
    was_slave=${node_slaves[$n]:=0}
    is_slave=0
    if ! (is_master $n); then
      is_slave=1
    fi
    #echo "$n slave?: was $was_slave is $is_slave"
    if [[ was_slave == is_slave ]]; then
      repeat="repeat"
      echo -n "."
    fi
  done
done
echo "ready"

redis-cli -p 7000 cluster info
sleep 2
redis-cli -p 7000 cluster nodes | sort -k 2
