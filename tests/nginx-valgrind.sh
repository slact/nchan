#!/bin/zsh
valgrind --trace-children=yes --track-fds=no --track-origins=yes ./nginx -p ./ -c ./nginx.conf
