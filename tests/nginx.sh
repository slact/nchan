#!/bin/zsh
NGINX_CONFIG=`pwd`/nginx.conf
NGINX_TEMP_CONFIG=`pwd`/.nginx.thisrun.conf
NGINX_OPT=( -p `pwd`/ 
    -c $NGINX_TEMP_CONFIG
)
cp -fv $NGINX_CONFIG $NGINX_TEMP_CONFIG
VALGRIND_OPT=( --trace-children=yes --track-origins=yes --read-var-info=yes )
WORKERS=5
NGINX_DAEMON="off"
NGINX_CONF=""
ACCESS_LOG="/dev/null"
ERRLOG_LEVEL="notice"

_cacheconf="  proxy_cache_path /tmp levels=1:2 keys_zone=cache:1m; \\n  server {\\n       listen 8007;\\n       location / { \\n          proxy_cache cache; \\n      }\\n  }\\n"
echo $cacheconf

for opt in $*; do
  if [[ "$opt" = <-> ]]; then
    WORKERS=$opt
  fi
  case $opt in
    leak|leakcheck)
      VALGRIND_OPT+=("--leak-check=full" "--show-leak-kinds=all" "--track-fds=yes");;
    valgrind)
      valgrind=1;;
    alleyoop)
      alleyoop=1;;
    cache)
      CACHE=1;;
    access)
      ACCESS_LOG="/dev/stdout";;
    worker|one|single) 
      WORKERS=1
      ;;
    debug|kdbg)
      WORKERS=1
      NGINX_DAEMON="on"
      debugger=1
      ;;
    debuglog)
      ERRLOG_LEVEL="debug"
      ;;
  esac
done

conf_replace(){
    echo "$1 $2"
    sed "s|\($1\).*|\1 $2;|g" $NGINX_TEMP_CONFIG -i
}

ulimit -c unlimited

if [[ ! -z $NGINX_CONF ]]; then
    NGINX_OPT+=( -g "$NGINX_CONF" )
fi
#echo $NGINX_CONF
#echo $NGINX_OPT
echo "nginx $NGINX_OPT"
conf_replace "access_log" $ACCESS_LOG
conf_replace "error_log" "stderr $ERRLOG_LEVEL"
conf_replace "worker_processes" $WORKERS
conf_replace "daemon" $NGINX_DAEMON
conf_replace "working_directory" "\"$(pwd)\""
if [[ ! -z $CACHE ]]; then
  sed "s|\#cachetag.*|${_cacheconf}|g" $NGINX_TEMP_CONFIG -i
fi

if [[ $debugger == 1 ]]; then
  
  ./nginx $NGINX_OPT
  sleep 0.2
  master_pid=`cat /tmp/pushmodule-test-nginx.pid`
  child_pids=`pgrep -P $master_pid`
  kdbg_pids=()
  while read -r line; do
    sudo kdbg -p $line ./nginx &
    kdbg_pids+="$!"
  done <<< $child_pids
  echo "kdbg at $kdbg_pids"
  wait $kdbg_pids
  kill $master_pid
elif [[ $valgrind == 1 ]]; then
  mkdir ./coredump 2>/dev/null
  pushd ./coredump >/dev/null
  valgrind $VALGRIND_OPT ../nginx $NGINX_OPT
  popd >/dev/null
elif [[ $alleyoop == 1 ]]; then
  alleyoop ./nginx $NGINX_OPT
else
  ./nginx $NGINX_OPT
fi
