#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/construct_datasets/construct_datasets.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Bash arguments and environment variables
policy=$1
dataset=$2
reset=$3
ingest_empty=$4
shutdown=$5

# Set environment variables
export JAVA_HOME=/opt/java/java17/openjdk
export PATH=/opt/java/java17/openjdk/bin:$PATH

# Path variables
script_dir=/starvers_eval/scripts

shutdown() {
    echo "$(log_timestamp) ${log_level}:Kill process /jena-fuseki/fuseki-server.jar to shutdown Jena" >> $log_file
    pkill -f '/jena-fuseki/fuseki-server.jar'
    while ps -ef | grep -q '[j]ena-fuseki/fuseki-server.jar'; do
        sleep 1
    done
    echo "$(log_timestamp) ${log_level}:/jena-fuseki/fuseki-server.jar killed." >> $log_file
}

startup() {
    echo "$(log_timestamp) ${log_level}:Start database server in background..." >> $log_file
    cp /starvers_eval/configs/construct_datasets/jenatdb2_${policy}_${dataset}/*.ttl /run/configuration
    nohup /jena-fuseki/fuseki-server --port=3030 --tdb2 &

    # Wait until server is up
    echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
    while [[ $(curl -I http://Starvers:3030 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '200' ]]; do
        sleep 1s
    done
    echo "$(log_timestamp) ${log_level}:Fuseki server is up" >> $log_file
}

if [[ "$reset" == "true" ]]; then
    if ps aux | grep '[j]ena-fuseki/fuseki-server.jar' >/dev/null; then
        shutdown
    fi
    
    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf /starvers_eval/databases/construct_datasets/jenatdb2/${policy}_${dataset}
    rm -rf /starvers_eval/configs/construct_datasets/jenatdb2_${policy}_${dataset}
    rm -rf /run/configuration
    rm -rf /tmp/*

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p /starvers_eval/databases/construct_datasets/jenatdb2/${policy}_${dataset}
    mkdir -p /starvers_eval/configs/construct_datasets/jenatdb2_${policy}_${dataset}
    mkdir -p /run/configuration

    echo "$(log_timestamp) ${log_level}:Parametrize and copy config file..." >> $log_file
    repositoryID=${policy}_${dataset}
    cp ${script_dir}/3_construct_datasets/configs/jenatdb2-config_template.ttl /starvers_eval/configs/construct_datasets/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
    sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/construct_datasets/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
fi

if [[ "$ingest_empty" == "true" ]]; then
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    /jena-fuseki/tdbloader2 --loc /starvers_eval/databases/construct_datasets/jenatdb2/${repositoryID} /starvers_eval/rawdata/${dataset}/empty.nt
fi

if [[ "$shutdown" == "true" ]]; then
    shutdown
fi

startup