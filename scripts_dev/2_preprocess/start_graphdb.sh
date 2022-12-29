#!/bin/bash
# Input parametrers
policy=$1
dataset=$2
reset=$3
ingest_empty=$4
shutdown=$5

# Set variables
script_dir=/starvers_eval/scripts
log_file=/starvers_eval/output/logs/preprocessing/construct_datasets.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"
#graphdb_port=$((7200))
export JAVA_HOME=/opt/java/openjdk
export PATH=/opt/java/openjdk/bin:$PATH
export GDB_JAVA_OPTS="$GDB_JAVA_OPTS -Dgraphdb.home.data=/starvers_eval/databases/preprocessing/graphdb_${policy}_${dataset}/data"

if [ $reset == "true" ]; then
    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf /starvers_eval/databases/preprocessing/*
    rm -rf /starvers_eval/configs/preprocessing/graphdb_${policy}_${dataset}

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p /starvers_eval/configs/preprocessing/graphdb_${policy}_${dataset}

    echo "$(log_timestamp) ${log_level}:Parametrize and copy config file..." >> $log_file
    repositoryID=${policy}_${dataset}
    cp ${script_dir}/2_preprocess/configs/graphdb-config_template.ttl /starvers_eval/configs/preprocessing/graphdb_${policy}_${dataset}/graphdb-config.ttl
    sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/preprocessing/graphdb_${policy}_${dataset}/graphdb-config.ttl
fi

if [ $ingest_empty == "true" ]; then
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    /opt/graphdb/dist/bin/preload -c /starvers_eval/configs/preprocessing/graphdb_${policy}_${dataset}/graphdb-config.ttl /starvers_eval/rawdata/${dataset}/empty.nt --force
fi

if [ $shutdown == "true" ]; then
    echo "$(log_timestamp) ${log_level}:Kill process /opt/java/openjdk/bin/java to shutdown GraphDB" >> $log_file
    pkill -f /opt/java/openjdk/bin/java
fi

echo "$(log_timestamp) ${log_level}:Start database server in background..." >> $log_file
/opt/graphdb/dist/bin/graphdb -d -s

# Wait until server is up
# GraphDB doesn't deliver HTTP code 200 for some reason ...
echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
while [[ $(curl -I http://Starvers:7200 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '406' ]]; do
    sleep 1s
done
echo "$(log_timestamp) ${log_level}:GraphDB server is up" >> $log_file