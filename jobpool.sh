#!/bin/bash
/usr/local/bin/jobpool \
--data-dir=/home/jobpool \
--name=${MY_POD_NAME} \
--listen-peer-urls=http://0.0.0.0:2380 \
--listen-client-urls=http://0.0.0.0:2379 \
--advertise-client-urls=http://${MY_POD_NAME}.${SERVICE_NAME}.${CLUSTER_NAMESPACE}:2379 \
--initial-advertise-peer-urls=http://${MY_POD_NAME}.${SERVICE_NAME}.${CLUSTER_NAMESPACE}:2380 \
--initial-cluster=${INITIAL_CLUSTER}