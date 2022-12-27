#!/bin/zsh

if [ "$#" -ne 2 ]
then
    echo "Usage: ./multiconn.sh <interface_number> <channel_string>"
fi

IFNUMBER=$1 # about 28232 connections each interface
CHANNEL=$2
I=0

while [ $I -lt $((IFNUMBER-1)) ]
do
    sudo ifconfig lo:$I 127.0.0.$((I+2)) netmask 255.0.0.0 up
    I=$((I+1))
done
gcc -o multiconn multiconn.c
./multiconn $IFNUMBER $CHANNEL