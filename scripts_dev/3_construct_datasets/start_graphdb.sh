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
export JAVA_HOME=/opt/java/openjdk
export PATH=/opt/java/openjdk/bin:$PATH
export GDB_JAVA_OPTS="$GDB_JAVA_OPTS -Dgraphdb.home.data=/starvers_eval/databases/construct_datasets/graphdb"

# Path variables
script_dir=/starvers_eval/scripts

shutdown() {
    echo "$(log_timestamp) ${log_level}:Kill process /opt/java/openjdk/bin/java to shutdown GraphDB" >> $log_file
    pkill -f /opt/java/openjdk/bin/java
    while ps -ef | grep -q '[o]pt/java/openjdk/bin/java'; do
        sleep 1
    done
    echo "$(log_timestamp) ${log_level}:/opt/java/openjdk/bin/java killed." >> $log_file
}

if [[ "$reset" == "true" ]]; then
    if ps aux | grep '[o]pt/java/openjdk/bin/java' >/dev/null; then
        shutdown
    fi

    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf /starvers_eval/databases/construct_datasets/graphdb/repositories/${policy}_${dataset}
    rm -rf /starvers_eval/configs/construct_datasets/graphdb_${policy}_${dataset}
    rm -rf /tmp/*

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p /starvers_eval/databases/construct_datasets/graphdb
    mkdir -p /starvers_eval/configs/construct_datasets/graphdb_${policy}_${dataset}

    echo "$(log_timestamp) ${log_level}:Parametrize and copy config file..." >> $log_file
    repositoryID=${policy}_${dataset}
    cp ${script_dir}/3_construct_datasets/configs/graphdb-config_template.ttl /starvers_eval/configs/construct_datasets/graphdb_${policy}_${dataset}/graphdb-config.ttl
    sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/construct_datasets/graphdb_${policy}_${dataset}/graphdb-config.ttl
fi

if [[ "$ingest_empty" == "true" ]]; then
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    /opt/graphdb/dist/bin/importrdf preload --force -c /starvers_eval/configs/construct_datasets/graphdb_${policy}_${dataset}/graphdb-config.ttl /starvers_eval/rawdata/${dataset}/empty.nt
fi

if [[ "$shutdown" == "true" ]]; then
    shutdown
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