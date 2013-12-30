#!/bin/zsh
target=$1
if [ -z $target ]; then
  target=$(realpath ./nginx)
fi

core_dir="./coredump"
mkdir $core_dir 2>/dev/null

echo "getting coredump for $target..."
dump=`mktemp --suffix=".core" -p $core_dir`
sudo systemd-coredumpctl dump $target > $dump
kdbg ./nginx "$dump" 2>/dev/null
rm "$dump"
