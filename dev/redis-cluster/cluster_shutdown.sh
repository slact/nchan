#!/bin/bash
echo "The process will shut down and will lose all the data to process press Y , to cancel press any button apart from Y"
read value

if [ $value == "y" ] || [ $value == "Y" ]
then
  ps -ef | grep redis-server | awk '{print $2}' | xargs kill -9
fi
