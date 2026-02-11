#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/ostrich_mgmt.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"


#######################################################################
# Functions
#######################################################################
startup() {
    echo "$(log_timestamp) ${log_level}:Start Ostrich node in background..." >> $log_file
    node /opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js -p 42564 -h 0.0.0.0 -t 480 ostrichFile@/starvers_eval/databases/ostrich/ostrich_${dataset} & 

    # Wait until server is up
    echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
    while [[ $(curl -I http://Starvers:42564 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '406' ]]; do
        sleep 1s
    done
    echo "$(log_timestamp) ${log_level}:Ostrich node is up" >> $log_file
}

shutdown() {
    echo "$(log_timestamp) ${log_level}:Kill process ${JAVA_HOME}/bin/java to shutdown GraphDB" >> $log_file
    pkill -f '/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js'
    while ps -ef | grep -q '[h]ttp.js'; do
            sleep 1
        done
        echo "$(log_timestamp) ${log_level}:/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js killed." >> $log_file
    done
}

dump_repo() {
    # TODO: Preserve graph/version info
    repositoryID=ostrich_${dataset}
    echo "$(log_timestamp) ${log_level}: Dumping repository ${repositoryID} to ${output_file}..." >> $log_file

    curl -G "http://Starvers:42564/sparql" \
        --data-urlencode "query=CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ?g { ?s ?p ?o } }" \
        -H "Accept: application/n-quads" \
        -o "${output_file}"
}


create_env() {
    if pgrep -f "/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js" >/dev/null; then
        shutdown
    fi

    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf ${database_dir}/ostrich_${dataset}
    rm -rf /tmp/*

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p ${database_dir}/ostrich_${dataset}

}

ingest_empty() {
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    cd ${database_dir}/ostrich_${dataset} && /opt/ostrich/ostrich-evaluate ingest never 0 /starvers_eval/rawdata/${dataset}/empty.nt 1 1 
}

#######################################################################
# Workflow
#######################################################################
set -euo pipefail

# Set environment variables
# No env variables for Ostrich


if [[ ${1:-} == "startup" ]]; then
    if [[ $# -ne 2 ]]; then
        echo "Usage: $0 startup <database_dir>"
        exit 1
    fi

    database_dir=$2

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

    create_env

elif [[ ${1:-} == "dump_repo" ]]; then
    if [[ $# -ne 5 ]]; then
        echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <output_file>"
        exit 1
    fi

    database_dir=$2
    policy=$3
    dataset=$4
    output_file=$5

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

    ingest_empty

else
    echo "Usage: $0 startup <database_dir>"
    echo "Usage: $0 shutdown"
    echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"
    echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <output_file>"
    echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"
    
    exit 1
fi
