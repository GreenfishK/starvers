#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/graphdb_mgmt.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"


#######################################################################
# Functions
#######################################################################
startup() {
    echo "$(log_timestamp) ${log_level}:Updating query timeouts to 30 sec ..." >> $log_file
    # export GDB_JAVA_OPTS="$GDB_JAVA_OPTS -Dhealth.max.query.time.seconds=30"
	
    repositoryID=${policy}_${dataset}
    echo "$(log_timestamp) ${log_level}:Start database server in background..." >> $log_file
    nohup /root/.cargo/bin/oxigraph serve --location ${database_dir}/${repositoryID} &
    
    # Wait until server is up
    echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
    while [[ $(curl -I http://Starvers:7878 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '406' ]]; do
        sleep 1s
    done

    # Save process ID
    echo "$(log_timestamp) ${log_level}: Obtaining Onyxgraph PID ..."  >> $log_file
    db_pid=$(pgrep -f "oxigraph serve --location ${database_dir}/${repositoryID}" | head -n 1)
    echo $db_pid > /tmp/onyxgraph_${policy}_${dataset}.pid
    echo "$(log_timestamp) ${log_level}: Onyxgraph PID is: ${db_pid}"  >> $log_file
    
    echo "$(log_timestamp) ${log_level}:Onyxgraph server is up" >> $log_file
}

shutdown() {
    echo "$(log_timestamp) ${log_level}:Shutdown Onyxgraph start" >> "$log_file"

    # --------------------------------------------------
    # Locate PID file
    # --------------------------------------------------
    pidfile=$(ls /tmp/graphdb_*.pid 2>/dev/null | head -n 1)

    if [ -z "$pidfile" ]; then
        echo "$(log_timestamp) ${log_level}:No PID file found, attempting fallback pkill" >> "$log_file"
        pkill -9 -f '/root/.cargo/bin/oxigraph serve' 2>/dev/null || true
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
    # echo "$(log_timestamp) ${log_level}:Waiting 2 seconds for JVM cleanup" >> "$log_file"
    # sleep 2

    # --------------------------------------------------
    # Wait for port release (Onyxgraph default: 7878)
    # --------------------------------------------------
    for i in {1..30}; do
        if ! ss -ltnp | grep -q ':7878'; then
            echo "$(log_timestamp) ${log_level}:Port 7878 released after $i seconds" >> "$log_file"
            break
        fi
        echo "$(log_timestamp) ${log_level}:Waiting for port 7878 to be released ($i/30)" >> "$log_file"
        sleep 1
    done

    # Final check
    if ss -ltnp | grep -q ':7878'; then
        echo "$(log_timestamp) ${log_level}:WARNING - Port 7878 still in use after timeout" >> "$log_file"
    fi

    echo "$(log_timestamp) ${log_level}:Shutdown Onyxgraph complete" >> "$log_file"
}

dump_repo() {
    repositoryID=${policy}_${dataset}
    echo "$(log_timestamp) ${log_level}:Dumping the repository ${repositoryID} to ${output_file}..." >> $log_file

    #curl "http://Starvers:7878/query" \
    #    -X GET \
    #    --header "Accept: application/x-trigstar" \
    #    -o "${output_file}"  

}

create_env() {
    # if pgrep -f "${JAVA_HOME}/bin/java" >/dev/null; then
    #     shutdown
    # fi
    # repositoryID=${policy}_${dataset}

    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf ${database_dir}/${repositoryID}
    # rm -rf ${config_dir}/onyxgraph/${repositoryID}
    # rm -rf /tmp/*

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p ${database_dir}/${repositoryID}
    # mkdir -p ${config_dir}/onyxgraph/${repositoryID}

    # echo "$(log_timestamp) ${log_level}:Parametrize and copy config file..." >> $log_file
    # cp ${config_tmpl_dir}/onyxgraph-config_template.ttl ${config_dir}/onyxgraph/${repositoryID}/${repositoryID}.ttl
    # sed -i "s/{{repositoryID}}/$repositoryID/g" ${config_dir}/onyxgraph/${repositoryID}/${repositoryID}.ttl


}

ingest() {
    echo "$(log_timestamp) ${log_level}:Ingest dataset ${dataset} for policy ${policy} into Onyxgraph" >> $log_file
    /root/.cargo/bin/oxigraph load --location ${database_dir}/${repositoryID} --file ${dataset_dir_or_file}
}

ingest_empty() {
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    /root/.cargo/bin/oxigraph load --location ${database_dir}/${repositoryID} --file /starvers_eval/rawdata/${dataset}/empty.nt  >> $log_file 2>&1
}

#######################################################################
# Workflow
#######################################################################
# Bash arguments and environment variables
set -euo pipefail

# Set environment variables
# export ...
# export ...

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

    # export ...

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

    # export ...

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

    # export ...

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

    # export ...

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

    # export ...

    ingest
else
    echo "Usage: $0 startup <database_dir> <policy> <dataset>"
    echo "Usage: $0 shutdown"
    echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"
    echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <output_file>"
    echo "Usage: $0 ingest <database_dir> <policy> <dataset> <config_dir>"
    echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"
    
    exit 1
fi
