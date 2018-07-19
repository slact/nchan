#!/bin/zsh
target=$1
core_dir="./coredump"
if [ -z $target ]; then
  target=$(realpath ./nginx)
  dump=$core_dir/last.core
else
  dump=$core_dir/$target.core
fi

mkdir $core_dir 2>/dev/null

echo "saving coredump for $target at $dump"

sudo coredumpctl dump $target > $dump
sudo kdbg ./nginx "$dump" 2>/dev/null
# rm "$dump" #keep it around for now
