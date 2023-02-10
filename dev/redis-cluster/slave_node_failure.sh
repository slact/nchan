#!/bin/bash

#Get start port From cluster Information
startPort=`cat cluster_information.json |jq '.start_port'`

#Get Password From cluster Information
password=`cat cluster_information.json |jq '.password'`
password="${password#?}"
password="${password%?}"

#Show Cluster Nodes
if [ ! -z "$password" -a "$password" != " " ] ;then
        redis-cli -p $startPort -a $password cluster nodes
        echo "Replica  Node Ports :"
        #Replica Node Ports
        redis-cli -p $startPort -a $password --no-auth-warning cluster nodes  |grep slave|cut -d ':' -f2|cut -d'-' -f1|cut -d '@' -f2|cut -c 2-

else
        redis-cli -p $startPort cluster nodes
        echo "Replica  Node Ports :"
        #Replica Node Ports
        redis-cli -p $startPort cluster nodes |grep slave|cut -d ':' -f2|cut -d'-' -f1|cut -d '@' -f2|cut -c 2-

fi

#Enter Replica Port Number for Failover
echo "Enter the port for failover"
read port_number

#Process Id of the particular Port
process_id=$(ps -ef |grep redis-server |grep $port_number |awk '{print $2}')

#Show the cluster Nodes
if [ ! -z "$process_id" ]
then
      kill -9 $process_id
      if [ ! -z "$password" -a "$password" != " " ]; then
              #Show Cluster Nodes
              redis-cli -p $startPort -a $password --no-auth-warning cluster nodes

      else
              #Show Cluster Nodes
              redis-cli -p $startPort cluster nodes
      fi
else
      echo "This Node is a failed node or is not a slave node "
fi




