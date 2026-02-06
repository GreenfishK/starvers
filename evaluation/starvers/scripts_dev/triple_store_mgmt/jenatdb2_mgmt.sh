#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/jenatdb2_mgmt.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"


startup() {
    echo "$(log_timestamp) ${log_level}:Start database server in background..." >> $log_file
    cp ${config_dir}/jenatdb2_${policy}_${dataset}/*.ttl /run/configuration
    nohup /jena-fuseki/fuseki-server --port=3030 --tdb2 &

    # Wait until server is up
    echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
    while [[ $(curl -I http://Starvers:3030 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '200' ]]; do
        sleep 1s
    done
    echo "$(log_timestamp) ${log_level}:Fuseki server is up" >> $log_file
}


shutdown() {
    echo "$(log_timestamp) ${log_level}:Kill process /jena-fuseki/fuseki-server.jar to shutdown Jena" >> $log_file
    pkill -f '/jena-fuseki/fuseki-server.jar'
    while ps -ef | grep -q '[j]ena-fuseki/fuseki-server.jar'; do
        sleep 1
    done
    echo "$(log_timestamp) ${log_level}:/jena-fuseki/fuseki-server.jar killed." >> $log_file
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
    
    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf ${database_dir}/jenatdb2/${policy}_${dataset}
    rm -rf ${config_dir}/jenatdb2_${policy}_${dataset}
    rm -rf /run/configuration
    rm -rf /tmp/*

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p ${database_dir}/jenatdb2/${policy}_${dataset}
    mkdir -p ${config_dir}/jenatdb2_${policy}_${dataset}
    mkdir -p /run/configuration

    echo "$(log_timestamp) ${log_level}:Parametrize and copy config file..." >> $log_file
    repositoryID=${policy}_${dataset}
    cp ${config_tmpl_dir}/jenatdb2-config_template.ttl ${config_dir}/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
    sed -i "s/{{repositoryID}}/$repositoryID/g" ${config_dir}/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
}

ingest_empty() {
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    repositoryID=${policy}_${dataset}
    /jena-fuseki/tdbloader2 --loc ${database_dir}/jenatdb2/${repositoryID} /starvers_eval/rawdata/${dataset}/empty.nt
}


# Bash arguments and environment variables
set -euo pipefail

# Set environment variables
export JAVA_HOME=/opt/java/java17/openjdk
export PATH=/opt/java/java17/openjdk/bin:$PATH


if [[ ${1:-} == "startup" ]]; then
    if [[ $# -ne 3 ]]; then
        echo "Usage: $0 startup <policy> <dataset>"
        exit 1
    fi

    policy=$2
    dataset=$3

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
    echo "Usage: $0 startup <policy> <dataset>"
    echo "Usage: $0 shutdown"
    echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"
    echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <output_file>"
    echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"
    
    exit 1
fi
