#!/bin/zsh
TESTDIR=`pwd`
SRCDIR=$(readlink -m $TESTDIR/../src)
echo $TESTDIR $SRCDIR
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
ERROR_LOG="stderr"
ERRLOG_LEVEL="notice"
TMPDIR=""
MEM="32M"

REDIS_CONF="$TESTDIR/redis.conf"
REDIS_PORT=8537

_cacheconf="  proxy_cache_path _CACHEDIR_ levels=1:2 keys_zone=cache:1m; \\n  server {\\n       listen 8007;\\n       location / { \\n          proxy_cache cache; \\n      }\\n  }\\n"
echo $cacheconf

for opt in $*; do
  if [[ "$opt" = <-> ]]; then
    WORKERS=$opt
  fi
  case $opt in
    redis-persist)
      persist_redis=1;;
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
    errorlog)
      ERROR_LOG="errors.log"
      rm ./errors.log 2>/dev/null
      ;;
    lomem|lowmem|small)
      MEM="5M";;
    himem|highmem|large)
      MEM="256M";;
    verylowmem|tiny)
      MEM="1M";;
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
conf_replace "error_log" "$ERROR_LOG $ERRLOG_LEVEL"
conf_replace "worker_processes" $WORKERS
conf_replace "daemon" $NGINX_DAEMON
conf_replace "working_directory" "\"$(pwd)\""
conf_replace "push_max_reserved_memory" "$MEM"
if [[ ! -z $CACHE ]]; then
  sed "s|^\s*#cachetag.*|${_cacheconf}|g" $NGINX_TEMP_CONFIG -i
  tmpdir=`pwd`"/.tmp"
  mkdir $tmpdir 2>/dev/null
  sed "s|_CACHEDIR_|\"$tmpdir\"|g" $NGINX_TEMP_CONFIG -i
fi

#shutdown old redis
old_redis_pid=`pgrep -f "redis-server 127.0.0.1:$REDIS_PORT"`
if [[ ! -z $old_redis_pid ]] && [[ -z $persist_redis ]]; then
  kill $old_redis_pid
  wait $old_redis_pid
  sleep 1
fi
#start redis
if [[ -z $old_redis_pid ]] || [[ -z $persist_redis ]]; then
  redis-server $REDIS_CONF --port $REDIS_PORT &
  redis_pid=$!
  echo "started redis on port $REDIS_PORT with pid $redis_pid"
else
  echo "redis already running on port $REDIS_PORT with pid $old_redis_pid"
fi

kdbg_pids=()

TRAPINT() {
  if [[ -z $persist_redis ]]; then
    kill $redis_pid
    wait $redis_pid
  fi
  if [[ $debugger == 1 ]]; then
    sudo kill $kdbg_pids
  fi
}

if [[ $debugger == 1 ]]; then
  
  ./nginx $NGINX_OPT
  sleep 0.2
  master_pid=`cat /tmp/pushmodule-test-nginx.pid`
  child_pids=`pgrep -P $master_pid`
  ln -sf $TESTDIR/nginx $SRCDIR/nginx >/dev/null
  ln -sf  $TESTDIR/nginx-pushmodule/src/nginx/src/ $SRCDIR/nginx-source >/dev/null
  sudo echo "attaching kdbg..."
  while read -r line; do
    sudo kdbg -p $line $SRCDIR/nginx &
    kdbg_pids+="$!"
  done <<< $child_pids
  echo "kdbg at $kdbg_pids"
  wait $kdbg_pids
  kill $master_pid
  rm -f $SRCDIR/nginx $SRCDIR/nginx-source 2>/dev/null
elif [[ $valgrind == 1 ]]; then
  mkdir ./coredump 2>/dev/null
  pushd ./coredump >/dev/null
  valgrind $VALGRIND_OPT ../nginx $NGINX_OPT
  popd >/dev/null
elif [[ $alleyoop == 1 ]]; then
  alleyoop ./nginx $NGINX_OPT
else
  ./nginx $NGINX_OPT &
  wait $!
fi
