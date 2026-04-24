#!/bin/bash

# Logging variables
log_file=${RUN_DIR}/output/logs/jenatdb2_mgmt.txt
log_dir=$(dirname "$log_file")
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Ensure directory exists
mkdir -p "$log_dir"

# Ensure file exists
if [ ! -f "$log_file" ]; then
    echo "Create log file $log_file"
    > "$log_file"
fi



startup() {
    echo "$(log_timestamp) ${log_level}:Update query timeouts to 30 sec ..." >> $log_file
    ttl_file="/starvers_eval/configs/ingest/jenatdb2/${policy}_${dataset}/${policy}_${dataset}.ttl"
    sed -i 's/\(ja:cxtValue "\)0,0/\130000,30000/' "$ttl_file"

    echo "$(log_timestamp) ${log_level}:Start database server in background..." >> $log_file
    /jena-fuseki/fuseki-server --config=$ttl_file --port=3030 --tdb2 >> "$log_file" 2>&1 &
    db_pid=$!  # capture immediately
    echo "$(log_timestamp) ${log_level}:Fuseki PID is $db_pid" >> $log_file

    timeout=120
    elapsed=0
    until curl -s -X POST http://Starvers:3030/${policy}_${dataset}/sparql \
        -H "Content-Type: application/sparql-query" \
        --data "ASK {}" >/dev/null 2>&1
    do
        sleep 1
        elapsed=$((elapsed + 1))
        if [ $elapsed -ge $timeout ]; then
            echo "$(log_timestamp) ${log_level}:ERROR — Fuseki did not come up after ${timeout}s" >> $log_file
            exit 1
        fi
        if ! kill -0 $db_pid 2>/dev/null; then
            echo "$(log_timestamp) ${log_level}:ERROR — Fuseki process $db_pid died" >> $log_file
            exit 1
        fi
    done

    echo $db_pid > /tmp/jenatdb2_${policy}_${dataset}.pid
    echo "$(log_timestamp) ${log_level}:Fuseki server is up" >> $log_file
}


shutdown() {
    echo "$(log_timestamp) ${log_level}:Shutdown Jena start" >> "$log_file"

    # --------------------------------------------------
    # Locate PID file
    # --------------------------------------------------
    pidfile=$(ls /tmp/jenatdb2_*.pid 2>/dev/null | head -n 1)

    if [ -z "$pidfile" ]; then
        echo "$(log_timestamp) ${log_level}:No PID file found, attempting fallback kill" >> "$log_file"
        pkill -9 -f ${JAVA_HOME}/bin/java 2>/dev/null || true
    else
        PID=$(cat "$pidfile")
        echo "$(log_timestamp) ${log_level}:Found PID file $pidfile with PID $PID" >> "$log_file"

        # --------------------------------------------------
        # Check if process exists
        # --------------------------------------------------
        if ps -p "$PID" > /dev/null 2>&1; then
            echo "$(log_timestamp) ${log_level}:Process $PID is running, attempting kill -9" >> "$log_file"

            kill -9 "$PID" 2>/dev/null || true

            # --------------------------------------------------
            # Wait with timeout (avoid infinite loop!)
            # --------------------------------------------------
            for i in {1..10}; do
                if ! ps -p "$PID" > /dev/null 2>&1; then
                    echo "$(log_timestamp) ${log_level}:Process $PID terminated after $i seconds" >> "$log_file"
                    break
                fi
                echo "$(log_timestamp) ${log_level}:Waiting for PID $PID to terminate ($i/10)" >> "$log_file"
                sleep 1
            done

        else
            echo "$(log_timestamp) ${log_level}:Process $PID already not running" >> "$log_file"
        fi

        # Remove PID file
        rm -f "$pidfile"
        echo "$(log_timestamp) ${log_level}:Removed PID file $pidfile" >> "$log_file"
    fi

    # --------------------------------------------------
    # Give JVM time to release resources
    # --------------------------------------------------
    echo "$(log_timestamp) ${log_level}:Waiting 2 seconds for JVM cleanup" >> "$log_file"
    sleep 2

    # --------------------------------------------------
    # Wait for port release
    # --------------------------------------------------
    for i in {1..30}; do
        if ! ss -ltnp | grep -q ':3030'; then
            echo "$(log_timestamp) ${log_level}:Port 3030 released after $i seconds" >> "$log_file"
            break
        fi
        echo "$(log_timestamp) ${log_level}:Waiting for port 3030 to be released ($i/30)" >> "$log_file"
        sleep 1
    done

    # Final check
    if ss -ltnp | grep -q ':3030'; then
        echo "$(log_timestamp) ${log_level}:WARNING - Port 3030 still in use after timeout" >> "$log_file"
    fi

    echo "$(log_timestamp) ${log_level}:Shutdown Jena complete" >> "$log_file"
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
    /jena-fuseki/tdbloader2 --loc ${database_dir} ${RUN_DIR}/rawdata/${dataset}/empty.nt
}


#######################################################################
# Workflow
#######################################################################
# Bash arguments and environment variables
set -euo pipefail

# Set environment variables
export JAVA_HOME=/opt/java/java17/openjdk
export PATH=/opt/java/java17/openjdk/bin:$PATH

# --------------------------------------------------
# Optional arguments parsing
# --------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --log-file)
            log_file="$2"
            shift 2
            ;;
        *)
            break
            ;;
    esac
done

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
    echo "Usage: $0 startup <database_dir> <policy> <dataset>"
    echo "Usage: $0 shutdown"
    echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"
    echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <output_file>"
    echo "Usage: $0 ingest <database_dir> <dataset_dir_or_file> <policy> <dataset> <config_dir>"
    echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"
    
    exit 1
fi
