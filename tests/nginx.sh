#!/bin/zsh
NGINX_OPT=( -p ./ -c ./nginx.conf )
VALGRIND_OPT=( --trace-children=yes --track-fds=no --track-origins=yes )
WORKERS=5
for opt in $*; do
  case $opt in
    leak|leakcheck)
      VALGRIND_OPT+=("--leak-check=full")
      ;;
    valgrind)
      valgrind=1
      ;;
    worker|one|single) 
      WORKERS=1
      ;;
  esac
done
NGINX_OPT+=( -g "worker_processes $WORKERS;" )

if [[ $valgrind == 1 ]]; then
  valgrind $VALGRIND_OPT ./nginx $NGINX_OPT
else
  ./nginx $NGINX_OPT
fi
