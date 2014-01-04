#!/bin/zsh

NGINX_OPT=( -p `pwd`/ 
    -c `pwd`/nginx.conf 
)
VALGRIND_OPT=( --trace-children=yes --track-fds=no --track-origins=yes --read-var-info=yes )
WORKERS=5
NGINX_DAEMON="off"
NGINX_CONF="working_directory \"`pwd`\"; "
ACCESS_LOG="\\/dev\\/null"
ERRLOG_LEVEL="notice"
for opt in $*; do
  if [[ "$opt" = <-> ]]; then
    WORKERS=$opt
  fi
  case $opt in
    leak|leakcheck)
      VALGRIND_OPT+=("--leak-check=full" "--show-leak-kinds=all");;
    valgrind)
      valgrind=1;;
    access)
      ACCESS_LOG="\\/dev\\/stdout";;
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
NGINX_CONF="worker_processes $WORKERS; daemon $NGINX_DAEMON; $NGINX_CONF"
NGINX_OPT+=( -g "$NGINX_CONF" )
#echo $NGINX_CONF
#echo $NGINX_OPT
echo "nginx $NGINX_OPT"

#set access log
echo $ACCESS_LOG
sed "s|\(access_log\).*|\1 $ACCESS_LOG;|g" ./nginx.conf -i
sed "s|\(^\s*error_log\s\+\S\+\).*|\1 $ERRLOG_LEVEL;|g" ./nginx.conf -i
if [[ $debugger == 1 ]]; then
  ./nginx $NGINX_OPT
  sleep 0.2
  master_pid=`cat /tmp/nhpm-test-nginx.pid`
  child_pid=`pgrep -P $master_pid`
  sudo kdbg -p $child_pid ./nginx
  kill $master_pid
elif [[ $valgrind == 1 ]]; then
  pushd ./coredump >/dev/null
  valgrind $VALGRIND_OPT ../nginx $NGINX_OPT
  popd >/dev/null
else
  ./nginx $NGINX_OPT
fi
