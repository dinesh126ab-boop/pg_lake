#!/bin/bash

trap "echo 'Caught termination signal. Exiting...'; exit 0" SIGINT SIGTERM

minio server /data &

minio_pid=$!

while ! curl http://localhost:9000; do
    echo "Waiting for http://localhost:9000..."
    sleep 1
done

# set access key and secret key
mc alias set local http://localhost:9000 minioadmin minioadmin

# create test bucket
mc mb local/testbucket

wait $minio_pid
