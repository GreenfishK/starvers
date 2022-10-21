#!/bin/bash

policies=("ic") # cb tbsf tbsh tb
datasets=("bearb-day" "bearc") # bearb-day beara bearc
host_port_graphdb=7200
host_port_jenatdb2=3030
command=""

docker network create 4_evaluation_default
### GraphDB ##################################################################
# Start 20 docker containers
#for policy in ${policies[@]}; do
#    for dataset in ${datasets[@]}; do
#        dataset=$dataset policy=$policy host_port=$host_port_graphdb docker-compose up graphdb_run & 
#        host_port_graphdb=$(($host_port_graphdb+10))
#    done
#done
dataset=bearb-day policy=ic host_port=7200 docker-compose up -d graphdb_run & dataset=bearc policy=ic host_port=7210 docker-compose up -d graphdb_run & 



### JenaTDB2 #################################################################
# TODO: write procedure for jenatdb2
# Start 20 docker containers
# docker-compose up jenatdb2_run

### Evaluate ################################################################
# Run queries against each triplestore x dataset x policy, each stored in an own container (40 containers)
active_containers=0
while [ $active_containers -ne $((${#policies[@]} * ${#datasets[@]})) ]; do
    docker-compose logs graphdb_run | grep -w "Started GraphDB in workbench mode" > log.txt
    active_containers=$((`sed -n '$=' log.txt`))
done

dataset=dummy policy=dummy host_port=7200 docker-compose up --build evaluate

dataset=dummy policy=dummy host_port=7200 docker-compose down
docker network rm 4_evaluation_default
docker rmi -f $(docker images -f "dangling=true" -q).


# TODO: free heap space in graphdb by deactivating the repository after querying it
# start-console [OPTION] [repositoryID]
# /opt/graphdb-free/app/bin/console
# process1 = pexpect.spawnu("/opt/graphdb/dist/bin/console")
# process1.timeout = 25
# process1.sendline("Close {0}".format(repository_name))
# process1.sendline("yes")
# process1.expect("Closed repository '{0}'".format(repository_name))
# process1.close()
