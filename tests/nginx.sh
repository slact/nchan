#!/bin/zsh
NGINX_OPT=( -p ./ -c ./nginx.conf )
VALGRIND_OPT=( --trace-children=yes --track-fds=no --track-origins=yes )
WORKERS=5
NGINX_CONF="working_directory \"`pwd`\"; "
for opt in $*; do
  case $opt in
    leak|leakcheck)
      VALGRIND_OPT+=("--leak-check=full")
      ;;
    valgrind)
      valgrind=1
      ;;
    worker|1|one|single) 
      WORKERS=1
      ;;
  esac
done
NGINX_CONF="worker_processes $WORKERS; $NGINX_CONF"
NGINX_OPT+=( -g "$NGINX_CONF" )
#echo $NGINX_CONF
#echo $NGINX_OPT

if [[ $valgrind == 1 ]]; then
  valgrind $VALGRIND_OPT ./nginx $NGINX_OPT
else
  ./nginx $NGINX_OPT
fi
