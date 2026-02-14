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
    node /opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js -p 42564 -h 0.0.0.0 -t 480 ostrichFile@${database_dir} & 

    # Wait until server is up
    echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
    until curl -s -X POST http://Starvers:42564 \
        -H "Content-Type: application/sparql-query" \
        --data "ASK {}" >/dev/null 2>&1
    do
        sleep 1
    done

    # Save process ID
    db_pid=$!
    echo $db_pid > /tmp/ostrich_${policy}_${dataset}.pid

    echo "$(log_timestamp) ${log_level}:Ostrich node is up" >> $log_file
}

shutdown() {
    echo "$(log_timestamp) ${log_level}:Kill process $/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js to shutdown Ostrich" >> $log_file
    pkill -f '/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js'
    while ps -ef | grep -q '[h]ttp.js'; do
        sleep 1
    done
    echo "$(log_timestamp) ${log_level}:/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js killed." >> $log_file
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

    # Create database directory
    echo "$(log_timestamp) ${log_level}:Clean database directory ${database_dir}" >> $log_file
    rm -rf ${database_dir}
    rm -rf /tmp/*

    echo "$(log_timestamp) ${log_level}:Create database directory ${database_dir}" >> $log_file
    mkdir -p ${database_dir}

    # Create virtual environment via symlinks 
    raw_root="/starvers_eval/rawdata"
    vdir=/starvers_eval/rawdata/${dataset}/alldata_vdir

    # Extract needed config values from toml
    file_fmt_len=$(python3 - <<EOF
import tomli
with open("/starvers_eval/configs/eval_setup.toml","rb") as f:
    config = tomli.load(f)
print(config["datasets"]["$dataset"]["ic_basename_length"])
EOF
)
    echo "$(log_timestamp) ${log_level}: File format length: ${file_fmt_len}" >> $log_file

    first_snapshot=$(printf "%0${file_fmt_len}d.nt" 1)
    cb_src="${raw_root}/${dataset}/alldata.CB_computed.nt"
    ic_src="${raw_root}/${dataset}/alldata.IC.nt/${first_snapshot}"

    # Create symlinks
    rm -rf "$vdir"
    mkdir -p "$vdir/alldata.IC.nt"

    ln -s "$cb_src" "$vdir/alldata.CB.nt"
    ln -s "$ic_src" "$vdir/alldata.IC.nt/$first_snapshot"

    echo "$(log_timestamp) ${log_level}:Virtual directory created at $vdir with symlinks." >> $log_file

}

ingest() {
    if [[ -z "$versions" ]]; then
        versions=$(python3 - <<EOF
import tomli
with open("/starvers_eval/configs/eval_setup.toml","rb") as f:
    config = tomli.load(f)
qs_name = list(config["datasets"]["$dataset"]["query_sets"].keys())[0]
print(config["datasets"]["$dataset"]["query_sets"][qs_name]["policies"]["$policy"]["versions"])
EOF
)
    fi

    echo "$(log_timestamp) ${log_level}:Ingest dataset ${dataset} for policy ${policy} into Ostrich" >> $log_file
    cd ${database_dir} && /opt/ostrich/ostrich-evaluate ingest never 0 ${dataset_dir_or_file} 1 ${versions}
            
}

ingest_empty() {
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    cd ${database_dir} && /opt/ostrich/ostrich-evaluate ingest never 0 /starvers_eval/rawdata/${dataset}/empty.nt 1 1 
}

#######################################################################
# Workflow
#######################################################################
set -euo pipefail

# Set environment variables
# No env variables for Ostrich


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
    if [[ $# -lt 6 || $# -gt 7 ]]; then
        echo "Usage: $0 ingest <database_dir> <dataset_dir_or_file> <policy> <dataset> <config_dir> [versions]"
        exit 1
    fi

    database_dir=$2
    dataset_dir_or_file=$3
    policy=$4
    dataset=$5
    config_dir=$6
    versions=${7:-}  

    ingest
else
    echo "Usage: $0 startup <database_dir> <policy> <dataset>"
    echo "Usage: $0 shutdown"
    echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"
    echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <output_file>"
    echo "Usage: $0 ingest <database_dir> <dataset_dir_or_file> <policy> <dataset> <config_dir>"
    echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"
    
    exit 1
fi
