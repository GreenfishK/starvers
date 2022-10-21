#!/bin/sh

policies="ic" # cb tbsf tbsh tb
datasets="bearb-day bearc" # bearb-day beara bearc
host_port_graphdb=7200
host_port_jenatdb2=3030

### GraphDB ##################################################################
# Start 20 docker containers
for policy in ${policies[@]}; do
    for dataset in ${datasets[@]}; do
        dataset=$dataset policy=$policy host_port=$host_port_graphdb \
        docker-compose up graphdb_run
        $host_port_graphdb = $host_port_graphdb + 10

### JenaTDB2 #################################################################
# TODO: write procedure for jenatdb2
# Start 20 docker containers
# docker-compose up jenatdb2_run

### Evaluate ################################################################
# Run queries against each triplestore x dataset x policy, each stored in an own container (40 containers)
# docker-compose up evaluate