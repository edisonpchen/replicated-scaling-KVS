#! /bin/bash

#create docker subnet
docker network create --subnet=10.10.1.0/16 kv_subnet

#build image
docker build -t kvs:2.0 .

node_count=$1

if [[ $# -eq 0 ]] ; then
    echo "usage $0 <n: int>"
    exit 1
fi

ports=()
ips=()

for n in $(seq $(($node_count)) ); do
    name="kvs-replica$(($n))"
    port=$((6900 + $n))
    ip=10.10.1.$(($n))
    docker stop $name 1>/dev/null
    docker rm -f $name 1>/dev/null
    docker create \
    --net kv_subnet \
    --ip "$ip" \
    --name "$name"\
    --publish "$port:8080" \
    --env ADDRESS="$ip:8080" \
    kvs:2.0 1>/dev/null

    ports+=("$port")
    ips+=("$ip:8080")

done

rm -f tests/metadata/*.json

echo ${ports[@]} > tests/metadata/ports.txt
echo ${ips[@]} > tests/metadata/ips.txt
