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
        echo "Node Ports :"
        redis-cli -p $startPort -a $password --no-auth-warning cluster nodes  |cut -d ':' -f2|cut -d'-' -f1|cut -d '@' -f2|cut -c 2-

else
        redis-cli -p $startPort cluster nodes
        echo "Nodes Ports :"
        redis-cli -p $startPort cluster nodes |cut -d ':' -f2|cut -d'-' -f1|cut -d '@' -f2|cut -c 2-

fi

#Enter the Node Port for process id check
echo "Enter the port for timeout followed by failover"
read port_number


if [[ ! -z "$port_number" ]]
then
  #process Id for the particular port
  process_id=$(ps -ef |grep redis-server |grep $port_number |awk '{print $2}')
else
    echo "Port Number required for timeout "
    exit 0
fi


kill -SIGSTOP  $process_id

sleep 10
kill -SIGCONT $process_id

#Show Cluster Nodes
if [ ! -z "$password" -a "$password" != " " ] ;then
        redis-cli -p $startPort -a $password cluster nodes

else
        redis-cli -p $startPort cluster nodes

fi