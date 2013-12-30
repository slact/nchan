#!/bin/zsh
NGINX_OPT=( -p ./ -c ./nginx.conf )
VALGRIND_OPT=( --trace-children=yes --track-fds=no --track-origins=yes )
WORKERS=5
NGINX_DAEMON="off"
NGINX_CONF="working_directory \"`pwd`\"; "
for opt in $*; do
  if [[ "$opt" = <-> ]]; then
    WORKERS=$opt
  fi
  case $opt in
    leak|leakcheck)
      VALGRIND_OPT+=("--leak-check=full" "--show-leak-kinds=all")
      ;;
    valgrind)
      valgrind=1
      ;;
    worker|one|single) 
      WORKERS=1
      ;;
    debug|kdbg)
      WORKERS=1
      NGINX_DAEMON="on"
      debugger=1
      ;;
  esac
done
NGINX_CONF="worker_processes $WORKERS; daemon $NGINX_DAEMON; $NGINX_CONF"
NGINX_OPT+=( -g "$NGINX_CONF" )
#echo $NGINX_CONF
#echo $NGINX_OPT
echo "nginx $NGINX_OPT"

if [[ $debugger == 1 ]]; then
  ./nginx $NGINX_OPT
  sleep 0.2
  master_pid=`cat /tmp/nhpm-test-nginx.pid`
  child_pid=`pgrep -P $master_pid`
  sudo kdbg -p $child_pid ./nginx
  kill $master_pid
elif [[ $valgrind == 1 ]]; then
  valgrind $VALGRIND_OPT ./nginx $NGINX_OPT
else
  ./nginx $NGINX_OPT
fi

