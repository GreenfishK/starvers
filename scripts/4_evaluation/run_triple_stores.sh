#!/bin/bash

policies=("ic") # cb tbsf tbsh tb
datasets=("bearb-day") # bearb-day beara bearc
triple_stores=("jenatdb2") # jenatdb2
host_port_graphdb=7200
host_port_jenatdb2=3030
command=""

#docker network create 4_evaluation_default
# Start containers with their respective policy, dataset and triple store
for triple_store in ${triple_stores[@]}; do
    for policy in ${policies[@]}; do
        for dataset in ${datasets[@]}; do
            docker-compose up ${triple_store}_run_${policy}_${dataset} 
            # docker-compose up -d jenatdb2_run_${policy}_${dataset} 
        done
    done
done

### Evaluate ################################################################
# Check if all GraphDB (and JenaTDB2 (TODO)) instances are running.
#active_containers=0
#while [ $active_containers -ne $((${#policies[@]} * ${#datasets[@]})) ]; do
#    docker-compose logs | grep -w "Started GraphDB in workbench mode" > log.txt
#    active_containers=$((`sed -n '$=' log.txt`))
#done

#docker-compose up --build evaluate
#docker-compose down
#docker network rm 4_evaluation_default
#docker rmi -f $(docker images -f "dangling=true" -q).

# TODO: free heap space in graphdb by deactivating the repository after querying it

