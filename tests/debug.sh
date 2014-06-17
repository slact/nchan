#!/bin/zsh
if [[ "$1" = <-> ]]; then
    sudo kdbg -p $1 ./nginx
else
    kdbg ./nginx $1
fi
