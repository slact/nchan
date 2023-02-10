#!/bin/bash

#Get start port From cluster Information
startPort=`cat cluster_information.json |jq '.start_port'`

#Get Password From cluster Information
password=`cat cluster_information.json |jq '.password'`
password="${password#?}"
password="${password%?}"

#Show the cluster Nodes
if [ ! -z "$password" -a "$password" != " " ] ;then
        redis-cli -p $startPort -a $password cluster nodes
        #Master Node Ports
        echo "Primary Node Ports :"
        redis-cli -p $startPort -a $password --no-auth-warning cluster nodes  |grep master|cut -d ':' -f2|cut -d'-' -f1|cut -d '@' -f2|cut -c 2-

else
        redis-cli -p $startPort cluster nodes
        echo "Primary Nodes Ports :"
        #Master Node Ports
        redis-cli -p $startPort cluster nodes |grep master|cut -d ':' -f2|cut -d'-' -f1|cut -d '@' -f2|cut -c 2-

fi

#Enter the Node Port for process id check
echo "Enter the port for failover"
read port_number

#process Id for the particular port
process_id=$(ps -ef |grep redis-server |grep $port_number |awk '{print $2}')


if [ ! -z "$process_id" ]
then
      #Kill the process by process id
      kill -9 $process_id
      if [ ! -z "$password" -a "$password" != " " ]; then
              #Show the cluster Nodes
              redis-cli -p $startPort -a $password --no-auth-warning cluster nodes

      else
              #Show the cluster Nodes
              redis-cli -p $startPort cluster nodes
      fi
else
      echo "This Node is a failed node or is not a master node "
fi




