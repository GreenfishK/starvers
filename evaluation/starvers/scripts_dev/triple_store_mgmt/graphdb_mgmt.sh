#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/graphdb_mgmt.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"


startup() {
    echo "$(log_timestamp) ${log_level}:Start database server in background..." >> $log_file
    /opt/graphdb/dist/bin/graphdb -d -s

    # Wait until server is up
    # GraphDB doesn't deliver HTTP code 200 for some reason ...
    echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
    while [[ $(curl -I http://Starvers:7200 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '406' ]]; do
        sleep 1s
    done
    echo "$(log_timestamp) ${log_level}:GraphDB server is up" >> $log_file
}

shutdown() {
    echo "$(log_timestamp) ${log_level}:Kill process ${JAVA_HOME}/bin/java to shutdown GraphDB" >> $log_file
    pkill -f ${JAVA_HOME}/bin/java
    while pgrep -f "${JAVA_HOME}/bin/java" >/dev/null; do
        sleep 1
    done
    echo "$(log_timestamp) ${log_level}:${JAVA_HOME}/bin/java killed." >> $log_file
}


dump_repo() {
    repositoryID=${policy}_${dataset}
    echo "$(log_timestamp) ${log_level}:Dumping the repository ${repositoryID} to ${data_dir}..." >> $log_file

    curl 'http://Starvers:7200/repositories/${repositoryID}/statements' \
        --header 'Accept: text/turtle-star \
        -O ${data_dir}/alldata.TB_star_hierarchical.ttl'
}

create_env() {
    if pgrep -f "${JAVA_HOME}/bin/java" >/dev/null; then
        shutdown
    fi

    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf ${database_dir}/repositories/${policy}_${dataset}
    rm -rf ${config_dir}/graphdb_${policy}_${dataset}
    rm -rf /tmp/*

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p ${database_dir}/repositories/${policy}_${dataset}
    mkdir -p ${config_dir}/graphdb_${policy}_${dataset}

    echo "$(log_timestamp) ${log_level}:Parametrize and copy config file..." >> $log_file
    repositoryID=${policy}_${dataset}
    cp ${config_tmpl_dir}/graphdb-config_template.ttl ${config_dir}/graphdb_${policy}_${dataset}/graphdb-config.ttl
    sed -i "s/{{repositoryID}}/$repositoryID/g" ${config_dir}/graphdb_${policy}_${dataset}/graphdb-config.ttl

}

ingest_empty() {
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    /opt/graphdb/dist/bin/importrdf preload --force -c ${config_dir}/graphdb_${policy}_${dataset}/graphdb-config.ttl /starvers_eval/rawdata/${dataset}/empty.nt
}


# Bash arguments and environment variables
set -euo pipefail

# Set environment variables
export JAVA_HOME=/opt/java/java11/openjdk
export PATH=/opt/java/java11/openjdk/bin:$PATH


if [[ ${1:-} == "startup" ]]; then
    if [[ $# -ne 2 ]]; then
        echo "Usage: $0 startup <database_dir>"
        exit 1
    fi

    database_dir=$2
    export GDB_JAVA_OPTS="$GDB_JAVA_OPTS -Dgraphdb.home.data=${database_dir}"

    startup

elif [[ ${1:-} == "shutdown" ]]; then
    if [[ $# -ne 1 ]]; then
        echo "Usage: $0 shutdown"
        exit 1
    fi

    shutdown

elif [[ ${1:-} == "create_env" ]]; then
    if [[ $# -ne 6 ]]; then
        echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"
        exit 1
    fi

    policy=$2
    dataset=$3
    database_dir=$4
    config_tmpl_dir=$5
    config_dir=$6

    export GDB_JAVA_OPTS="$GDB_JAVA_OPTS -Dgraphdb.home.data=${database_dir}"

    create_env

elif [[ ${1:-} == "dump_repo" ]]; then
    if [[ $# -ne 5 ]]; then
        echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <data_dir>"
        exit 1
    fi

    database_dir=$2
    policy=$3
    dataset=$4
    data_dir=$5

    export GDB_JAVA_OPTS="$GDB_JAVA_OPTS -Dgraphdb.home.data=${database_dir}"

    dump_repo

elif [[ ${1:-} == "ingest_empty" ]]; then
    if [[ $# -ne 5 ]]; then
        echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"
        exit 1
    fi

    database_dir=$2
    policy=$3
    dataset=$4
    config_dir=$5

    export GDB_JAVA_OPTS="$GDB_JAVA_OPTS -Dgraphdb.home.data=${database_dir}"

    ingest_empty

else
    echo "Usage: $0 startup <database_dir>"
    echo "Usage: $0 shutdown"
    echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"
    echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <data_dir>"
    echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"
    
    exit 1
fi
