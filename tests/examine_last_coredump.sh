#!/bin/zsh
nginx_path=`realpath ./nginx`
#dump last nginx
sudo systemd-coredumpctl dump $nginx_path > ./.coredump
kdbg ./nginx ./.coredump
rm ./.coredump
