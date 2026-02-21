#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/jenatdb2_mgmt.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"


startup() {
    echo "$(log_timestamp) ${log_level}:Start database server in background..." >> $log_file
    nohup /jena-fuseki/fuseki-server --config=/starvers_eval/configs/ingest/jenatdb2/${policy}_${dataset}/${policy}_${dataset}.ttl --port=3030 --tdb2 &
    
    # Wait until server is up
    echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
    until curl -s -X POST http://Starvers:3030/${policy}_${dataset}/sparql \
        -H "Content-Type: application/sparql-query" \
        --data "ASK {}" >/dev/null 2>&1
    do
        sleep 1
    done
    echo "$(log_timestamp) ${log_level}:Fuseki server is up" >> $log_file

    # Save process ID
    db_pid=$!
    echo $db_pid > /tmp/jenatdb2_${policy}_${dataset}.pid

}


shutdown() {
    echo "$(log_timestamp) ${log_level}:Kill process /jena-fuseki/fuseki-server.jar to shutdown Jena" >> $log_file
    pkill -f '/jena-fuseki/fuseki-server.jar'
    while ps -ef | grep -q '[j]ena-fuseki/fuseki-server.jar'; do
        sleep 1
    done

    while lsof -i :3030 >/dev/null 2>&1; do
    	echo "Waiting for port 3030 to be released..."
    	sleep 1
    done

    echo "$(log_timestamp) ${log_level}:/jena-fuseki/fuseki-server.jar killed and port 3030 released" >> $log_file
}


dump_repo() {
    repositoryID=${policy}_${dataset}
    echo "$(log_timestamp) ${log_level}:Dumping the repository ${repositoryID} to ${output_file}..." >> $log_file

    # TODO: Test this function

    # Run CONSTRUCT query
    /jena-fuseki/bin/s-query \
        --service "http://Starvers:3030/${repositoryID}/query" \
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX vers: <https://github.com/GreenfishK/DataCitation/versioning/>

        CONSTRUCT {
            ?triple vers:valid_until ?valid_until .
        }
        WHERE {
            BIND(<< <<?s ?p ?o>> vers:valid_from ?valid_from >> AS ?triple)
            ?triple vers:valid_until ?valid_until .
        }
        ORDER BY DESC(?valid_from)" \
        > "${output_file}"

    echo "$(log_timestamp) ${log_level}:Repository ${repositoryID} exported to Turtle-Star" >> $log_file

}

create_env() {
    if ps aux | grep '[j]ena-fuseki/fuseki-server.jar' >/dev/null; then
        shutdown
    fi
    repositoryID=${policy}_${dataset}

    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf ${database_dir}/${repositoryID}
    rm -rf ${config_dir}/jenatdb2/${repositoryID}
    rm -rf /run/configuration
    rm -rf /tmp/*

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p ${database_dir}/${repositoryID}
    mkdir -p ${config_dir}/jenatdb2/${repositoryID}
    mkdir -p /run/configuration

    echo "$(log_timestamp) ${log_level}:Parametrize and copy config file..." >> $log_file
    cp ${config_tmpl_dir}/jenatdb2-config_template.ttl ${config_dir}/jenatdb2/${repositoryID}/${repositoryID}.ttl
    sed -i "s/{{repositoryID}}/$repositoryID/g" ${config_dir}/jenatdb2/${repositoryID}/${repositoryID}.ttl
}

ingest() {
    echo "$(log_timestamp) ${log_level}:Ingest dataset ${dataset} for policy ${policy} into Jena TDB2" >> $log_file
    repositoryID=${policy}_${dataset}
    cd ${database_dir} && /jena-fuseki/tdbloader2 --loc ${database_dir} ${dataset_dir_or_file}

}

ingest_empty() {
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    repositoryID=${policy}_${dataset}
    /jena-fuseki/tdbloader2 --loc ${database_dir}/jenatdb2/${repositoryID} /starvers_eval/rawdata/${dataset}/empty.nt
}


#######################################################################
# Workflow
#######################################################################
# Bash arguments and environment variables
set -euo pipefail

# Set environment variables
export JAVA_HOME=/opt/java/java17/openjdk
export PATH=/opt/java/java17/openjdk/bin:$PATH


if [[ ${1:-} == "startup" ]]; then
    if [[ $# -ne 4 ]]; then
        echo "Usage: $0 startup <database_dir> <policy> <dataset>"
        exit 1
    fi

    database_dir=$2
    policy=$3
    dataset=$4

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

elif [[ ${1:-} == "ingest" ]]; then
    if [[ $# -ne 6 ]]; then
        echo "Usage: $0 ingest <database_dir> <dataset_dir_or_file> <policy> <dataset> <config_dir>"
        exit 1
    fi

    database_dir=$2
    dataset_dir_or_file=$3
    policy=$4
    dataset=$5
    config_dir=$6

    ingest
else
    echo "Usage: $0 startup <policy> <dataset>"
    echo "Usage: $0 shutdown"
    echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"
    echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <output_file>"
    echo "Usage: $0 ingest <database_dir> <policy> <dataset> <config_dir>"
    echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"
    
    exit 1
fi
