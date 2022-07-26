#!/bin/zsh
DEVDIR=`pwd`

#SRCDIR=$(readlink -m $DEVDIR/../src) #not available on platforms like freebsd
SRCDIR=`perl -e "use Cwd realpath; print realpath(\"$DEVDIR/../src\");"`

VALGRIND_OPT=( "--tool=memcheck" "--track-origins=yes" "--read-var-info=yes" )

VG_MEMCHECK_OPT=( "--leak-check=full" "--show-leak-kinds=all" "--leak-check-heuristics=all" "--keep-stacktraces=alloc-and-free" "--suppressions=${DEVDIR}/vg.supp" )

#expensive definedness checks (newish option)
VG_MEMCHECK_OPT+=( "--expensive-definedness-checks=yes")

#long stack traces
VG_MEMCHECK_OPT+=("--num-callers=20")

#generate suppresions
#VG_MEMCHECK_OPT+=("--gen-suppressions=all")

#track files
#VG_MEMCHECK_OPT+=("--track-fds=yes")



WORKERS=5
NGINX_DAEMON="off"
NGINX_CONF=""
ACCESS_LOG="/dev/null"
ERROR_LOG="stderr"
ERRLOG_LEVEL="notice"
TMPDIR=""
MEM="32M"

DEBUGGER_NAME="kdbg"
DEBUGGER_CMD="dbus-run-session kdbg -p %s $SRCDIR/nginx"

#DEBUGGER_NAME="nemiver"
#DEBUGGER_CMD="nemiver --attach=%s $SRCDIR/nginx"

REDIS_CONF="$DEVDIR/redis.conf"
REDIS_PORT=8537
REDIS_OPT=(--port $REDIS_PORT)

pushd "${DEVDIR}"/nginx-pkg/pkg/nginx-* >/dev/null
_pkgdir=`pwd`
popd > /dev/null

_dynamic_module="$_pkgdir/etc/nginx/modules/ngx_nchan_module.so"

_cacheconf="  proxy_cache_path _CACHEDIR_ levels=1:2 keys_zone=cache:1m; \\n  server {\\n       listen 8007;\\n       location / { \\n          proxy_cache cache; \\n      }\\n  }\\n"

NGINX_CONF_FILE="nginx.conf"

NGINX_VER=$($DEVDIR/nginx -v 2>&1)
NGINX_VER=${NGINX_VER:15}
redis_version=""

for opt in $*; do
  if [[ "$opt" = <-> ]]; then
    WORKERS=$opt
  fi
  case $opt in
    noredis|no-redis)
      no_redis=1;;
    redis=*)
      redis_version="${opt:6}"
      ;;
    redis-persist)
      persist_redis=1;;
    redis-tls)
      redis_tls=1
      REDIS_OPT=(--port 0 --tls-port $REDIS_PORT --tls-cert-file redis-tls/redis.crt --tls-key-file redis-tls/redis.key --tls-ca-cert-file redis-tls/ca.crt)
      ;;
    leak|leakcheck|valgrind|memcheck)
      valgrind=1
      VALGRIND_OPT+=($VG_MEMCHECK_OPT);;
    debug-memcheck)
      valgrind=1
      VALGRIND_OPT+=($VG_MEMCHECK_OPT)
      VALGRIND_OPT+=( "--vgdb=yes" "--vgdb-error=1" )
      #ATTACH_DDD=1
      ;;
    massif)
      VALGRIND_OPT=( "--tool=massif" "--heap=yes" "--stacks=yes" "--massif-out-file=massif-nginx-%p.out")
      valgrind=1
      ;;
    sanitize-undefined)
      FSANITIZE_UNDEFINED=1
      ;;
    callgrind|profile)
      VALGRIND_OPT=( "--tool=callgrind" "--collect-jumps=yes"  "--collect-systime=yes" "--branch-sim=yes" "--cache-sim=yes" "--simulate-hwpref=yes" "--simulate-wb=yes" "--callgrind-out-file=callgrind-nginx-%p.out")
      valgrind=1;;
    helgrind)
    VALGRIND_OPT=( "--tool=helgrind" "--free-is-write=yes")
    valgrind=1
    ;;
    cachegrind)
      VALGRIND_OPT=( "--tool=cachegrind" )
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
    debugmaster|debug-master)
      WORKERS=1
      debug_master=1
      NGINX_DAEMON="off"
      ;;
    devconf)
      NGINX_CONF_FILE="dev.conf"
      ;;
    debug)
      WORKERS=1
      NGINX_DAEMON="on"
      debugger=1
      ;;
    debug=*)
      debug_what="${opt:6}"
      if [[ $debug_what == "master" ]]; then
        WORKERS=1
        debug_master=1
        NGINX_DAEMON="off"
      else
        NGINX_DAEMON="on"
        debugger=1
        child_text_match=$debug_what
      fi
      ;;
    altport)
      ALTPORT=1
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
    sudo)
      SUDO="sudo";;
  esac
done

NGINX_CONFIG=`pwd`/$NGINX_CONF_FILE
NGINX_TEMP_CONFIG=`pwd`/.nginx.thisrun.conf
NGINX_OPT=( -p `pwd`/ 
    -c $NGINX_TEMP_CONFIG
)
cp -fv $NGINX_CONFIG $NGINX_TEMP_CONFIG

_sed_i_conf() {
  sed $1 $NGINX_TEMP_CONFIG > $NGINX_TEMP_CONFIG.tmp && mv $NGINX_TEMP_CONFIG.tmp $NGINX_TEMP_CONFIG
}

conf_replace(){
    echo "$1 $2"
    _sed_i_conf "s|^\( *\)\($1\)\( *\).*|\1\2\3$2;|g"
}

_semver_gteq() {
  ruby -rrubygems -e "exit Gem::Version.new(('$1').match(/\/?([.\d]+)/)[1]) < Gem::Version.new(('$2').match(/^[^\s+]/)) ? 0 : 1"
  return $?
}

ulimit -c unlimited

if [[ ! -z $NGINX_CONF ]]; then
    NGINX_OPT+=( -g "$NGINX_CONF" )
fi
#echo $NGINX_CONF
#echo $NGINX_OPT

export ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer
export ASAN_OPTIONS=symbolize=1

echo "nginx $NGINX_OPT"
if [[ ! -z $ALTPORT ]]; then
  _sed_i_conf "s|^\( *\)listen\( *\)\(.*\)|\1listen\21\3|g"
fi

conf_replace "access_log" $ACCESS_LOG
conf_replace "error_log" "$ERROR_LOG $ERRLOG_LEVEL"
conf_replace "worker_processes" $WORKERS
conf_replace "daemon" $NGINX_DAEMON
conf_replace "working_directory" "\"$(pwd)\""
conf_replace "push_max_reserved_memory" "$MEM"
if [[ ! -z $CACHE ]]; then
  _sed_i_conf "s|^ *#cachetag.*|${_cacheconf}|g"
  tmpdir=`pwd`"/.tmp"
  mkdir $tmpdir 2>/dev/null
  _sed_i_conf "s|_CACHEDIR_|\"$tmpdir\"|g"
fi

if (_semver_gteq $NGINX_VER 1.9.5); then
  #do nothing, http2 is on by default
elif (_semver_gteq $NGINX_VER 1.3.15); then
  _sed_i_conf "s|^\( *listen *8085 *\).*|\1 spdy;|g"
else
  _sed_i_conf "s|^\( *listen *8085 *\).*|\1;|g"
fi

if [[ -f "$_dynamic_module" ]]; then
  _sed_i_conf "s|^ *#load_module.*|load_module \"${_dynamic_module}\";|g"
fi

#shutdown old redis
old_redis_pid=`pgrep -f "redis.*-server 127.0.0.1:$REDIS_PORT"`
if [[ ! -z $old_redis_pid ]] && [[ -z $persist_redis ]]; then
  kill $old_redis_pid
  wait $old_redis_pid
  sleep 1
fi
#start redis
if [[ -z $no_redis ]]; then
  if [[ -z $old_redis_pid ]] || [[ -z $persist_redis ]]; then
    if [[ -z $persist_redis ]]; then
      redis${redis_version}-server $REDIS_CONF $REDIS_OPT &
      redis_pid=$!
    else
      redis${redis_version}-server $REDIS_CONF $REDIS_OPT --daemonize yes
      sleep 1
      redis_pid=$(cat /tmp/redis-pushmodule.pid)
    fi
    echo "started redis on port $REDIS_PORT with pid $redis_pid"
  else
    echo "redis already running on port $REDIS_PORT with pid $old_redis_pid"
  fi
else
  echo "don't start redis"
fi


ln -sf $DEVDIR/nginx $SRCDIR/nginx >/dev/null
ln -sf $DEVDIR/nginx-nchan/src/nginx/src/ $SRCDIR/nginx-source >/dev/null


debugger_pids=()

TRAPINT() {
  if [[ -z $persist_redis ]]; then
    kill $redis_pid
    wait $redis_pid
  fi
  if [[ $debugger == 1 ]]; then
    sudo kill $debugger_pids
  fi
}

attach_debugger() {
  master_pid=`cat /tmp/nchan-test-nginx.pid`
  while [[ -z $child_pids ]]; do
    if [[ -z $child_text_match ]]; then
      child_pids=`pgrep -P $master_pid`
    else
      child_pids=`pgrep -P $master_pid -f $child_text_match`
    fi
    sleep 0.1
  done
  while read -r line; do
    echo "attaching $1 to $line"
    sudo $(printf $2 $line) &
    debugger_pids+="$!"
  done <<< $child_pids
  echo "$1 at $debugger_pids"
}

attach_ddd_vgdb() {
  master_pid=$1
  echo "attaching DDD for vgdb to master process $master_pid"
  ddd --eval-command "set non-stop off" --eval-command "target remote | vgdb --pid=$master_pid" "$SRCDIR/nginx" 2>/dev/null &
  debugger_pids+="$!"
  sleep 1
  while [[ -z $child_pids ]]; do
    child_pids=`pgrep -P $master_pid`
    sleep 0.3
  done
  echo "child pids: $child_pids"
  
  while read -r line; do
    echo "attaching DDD for vgdb to $line"
    ddd --eval-command "set non-stop off" --eval-command "target remote | vgdb --pid=$line" "$SRCDIR/nginx" 2>/dev/null &
    debugger_pids+="$!"
  done <<< $child_pids
  echo "$1 at $debugger_pids"
}

if [[ ! -f ./nginx ]]; then
  echo "./nginx not found"
  exit 1
fi

if [[ $debugger == 1 ]]; then
  $SUDO ./nginx $NGINX_OPT
  if ! [ $? -eq 0 ]; then; 
    echo "failed to start nginx"; 
    exit 1
  fi
  sleep 0.2
  attach_debugger "$DEBUGGER_NAME" "$DEBUGGER_CMD"
  wait $debugger_pids
  kill $master_pid
elif [[ $debug_master == 1 ]]; then
  pushd $SRCDIR
  sudo kdbg -a "$NGINX_OPT" "./nginx"
  popd
elif [[ $valgrind == 1 ]]; then
  mkdir ./coredump 2>/dev/null
  pushd ./coredump >/dev/null
  if [[ $ATTACH_DDD == 1 ]]; then
    $SUDO valgrind $VALGRIND_OPT ../nginx $NGINX_OPT &
    _master_pid=$!
    echo "nginx at $_master_pid"
    sleep 4
    attach_ddd_vgdb $_master_pid
    wait $debugger_pids
    kill $master_pid
  else
    echo $SUDO valgrind $VALGRIND_OPT ../nginx $NGINX_OPT
    $SUDO valgrind $VALGRIND_OPT ../nginx $NGINX_OPT
  fi
  popd >/dev/null
elif [[ $alleyoop == 1 ]]; then
  alleyoop ./nginx $NGINX_OPT
else
  $SUDO ./nginx $NGINX_OPT &
  wait $!
fi
