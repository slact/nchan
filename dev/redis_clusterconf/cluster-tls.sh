#!/bin/zsh

for (( n=7000; n<=7005; n++ ))
do
    echo N=${n}
    m=$(expr ${n} + 3000)
    redis-server ./redis-${n}.conf --port ${n} --tls-port $m \
		 --tls-cert-file ../redis-tls/redis.crt \
		 --tls-key-file ../redis-tls/redis.key \
		 --tls-ca-cert-file ../redis-tls/ca.crt &
done

echo "all redis-servers in cluster started."

wait
