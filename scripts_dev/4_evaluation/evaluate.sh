#!/bin/bash

policies=("ic cb") # cb tbsf tbsh tb
datasets=("bearb-day") # bearb-day beara bearc
triple_stores=("jenatdb2") # jenatdb2

#docker network create 4_evaluation_default
# Start containers with their respective policy, dataset and triple store
for triple_store in ${triple_stores[@]}; do
    for policy in ${policies[@]}; do
        for dataset in ${datasets[@]}; do
            docker-compose up -d ${triple_store}_run_${policy}_${dataset} 
            > tmp_log.txt
            while [ $((`sed -n '$=' tmp_log.txt`)) -ne $((1)) ]; do
                if [ $triple_store == "graphdb" ]; then
                    docker-compose logs | grep -w "Started GraphDB in workbench mode" > tmp_log.txt
                elif [ $triple_store == "jenatdb2" ]; then
                    docker-compose logs | grep -e "Started .* UTC on port" > tmp_log.txt
                else echo "Triple store must be of of $triple_stores"
                fi
            done
            rm tmp_log.txt
            # TODO: save the file to the timestamped experiment directory
            triple_store=${triple_store} policy=${policy} dataset=${dataset} docker-compose up --build evaluate >> ~/.BEAR/output/logs/queries.txt
            docker-compose down 
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


#docker network rm 4_evaluation_default
docker rmi -f $(docker images -f "dangling=true" -q).

# TODO: free heap space in graphdb by deactivating the repository after querying it

