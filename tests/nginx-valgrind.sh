#!/bin/zsh
valgrind --trace-children=yes --track-fds=yes --track-origins=yes ../../nginx-nhpm/pkg/nginx-nhpm-dev/usr/bin/nginx -p ./ -c ./nginx.conf
