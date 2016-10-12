#!/bin/bash


server="127.0.0.1"
port="6379"
db="0"
dir=$(dirname $0)
pushd $dir > /dev/null
fulldir=$(pwd)
popd > /dev/null
  
while getopts "s:p:d:t:l:h" opt; do
  case $opt in
    s)
      server=$OPTARG;;
    p)
      port=$OPTARG;;
    d)
      db=$OPTARG;;
    t)
      interval=$OPTARG;;
    l)  
      logfile=$OPTARG;;
    h)
      echo "nchan redis server data consistency check"
      echo " -s [server]"
      echo " -p [port]"
      echo " -d [db number]"
      echo " -t [minutes]   repeat at this interval"
      echo " -l [logfile]   log results to this file"
      ;;
   \?)
     exit 1
     ;;
  esac
done

url="redis://$server:$port/$db"

if [[ $interval ]]; then
  echo "rsck $url every $interval minutes"
else
  echo "rsck $url"
fi

if [[ $logfile ]]; then
  echo "logging to $logfile"
fi

if [[ $interval ]]; then
  sleeptime=$(echo "$interval * 60" | bc)
fi
while true; do
  ret=$(redis-cli -p 8537 --eval $dir/../src/store/redis/scripts/rsck.lua --raw)
  echo $ret
  
  if [[ $logfile ]]; then
    printf "$(date)\n$ret\n\n" >> $logfile
  fi
  
  if [[ ! $interval ]]; then
    break
  else
    sleep $sleeptime
  fi
done
 
