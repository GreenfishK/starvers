#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/evaluate/query.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Bash arguments and environment variables
policies=("${policies}") 
datasets=("${datasets}") 
triple_stores=("${triple_stores}")
graphdb_port=$(("${graphdb_port}"))
jenatdb2_port=$(("${jenatdb2_port}"))
ostrich_port=$(("${ostrich_port}"))

function policy_allowed() {
    local triplestore=$1
    local policy=$2
    if [ "$triplestore" == "jenatdb2" ]; then
        return 0
    elif [ "$triplestore" == "graphdb" ]; then
        return 0
    elif [ "$triplestore" == "ostrich" ]; then
        if [ "$policy" == "ostrich" ]; then
            return 0
        else
            return 1
        fi
    else
        return 1
    fi

}

start_mem_tracker() {
    local pid=$1
    local label=$2
    local outfile=$3
    local interval=${4:-1}

    (
        echo "timestamp;label;pid;rss_gb"
        while kill -0 "$pid" 2>/dev/null; do
            rss_kb=$(ps -o rss= -p "$pid" | awk '{print $1}')
            ts=$(date --iso-8601=seconds)
            echo "${ts};${label};${pid};$(awk "BEGIN {print ${rss_kb}/1024/1024}")"
            sleep "$interval"
        done
    ) >> "$outfile" &
    
    echo $!   # return tracker PID
}


# Prepare directories and files
rm -rf /starvers_eval/output/logs/evaluate/
mkdir -p /starvers_eval/output/logs/evaluate/

# Prepare log, main memory, and time files
> $log_file

mem_file="/starvers_eval/output/measurements/memory_consumption.csv"
> $mem_file
echo "timestamp;label;pid;rss_gb" >> $mem_file

time_file="/starvers_eval/output/measurements/time.csv"
> $time_file
echo "triplestore;dataset;policy;query_set;snapshot;snapshot_ts;query;execution_time;snapshot_creation_time" >> $time_file

# Exclude queries
echo "Tag queries query8 and query9 from the BEARC complex query set 
for the cb_sr_ng policy for excluding their result sets 
from serialization." >> $log_file
root_dir="/starvers_eval/queries/final_queries/cb_sr_ng/bearc/complex"

for subdir in $(ls -d ${root_dir}/*); do
    for file in ${subdir}/query8_q5_v* ${subdir}/query9_q0_v*; do
        if ! head -n 1 ${file} | grep -q "# Exclude"; then
            sed -i '1i\# Exclude' ${file}
        fi
    done
done


# main loop
for triple_store in ${triple_stores[@]}; do
    if [ ${triple_store} == "ostrich" ]; then
        for dataset in ${datasets[@]}; do
            # Start database server and run in background
            node /opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js -p ${ostrich_port} -h 0.0.0.0 -t 480 ostrichFile@/starvers_eval/databases/ostrich/ostrich_${dataset} & 
            db_pid=$!
            echo "$(log_timestamp) ${log_level}:Starting Ostrich server for the evaluation..." >> $log_file
            
            # Wait until server is up
            echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file

            counter=0
            while [[ $(curl -I http://Starvers:${ostrich_port}/sparql 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '200' ]]; do
                sleep 1s
                counter=$((counter + 1))
                if [[ counter -ge 60 ]]; then
                    echo "$(log_timestamp) ${log_level}:Server not up after 60 seconds, restarting..." >> $log_file
                    pkill -f '/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js'
                    node /opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js -p ${ostrich_port} -h 0.0.0.0 -t 480 ostrichFile@/starvers_eval/databases/ostrich/ostrich_${dataset} & 
                    db_pid=$!
                    counter=0
                fi
            done
            echo "$(log_timestamp) ${log_level}:Ostrich server is up." >> $log_file

            # Start tracling memory consumption
            mem_tracker_pid=$(start_mem_tracker $db_pid "ostrich_${dataset}" $mem_file 0.5)
            echo "$(log_timestamp) ${log_level}:Started memory tracker with PID ${mem_tracker_pid} for Ostrich." >> $log_file

            # Clean output directory
            rm -rf /starvers_eval/output/result_sets/${triple_store}/ostrich_${dataset}

            # Evaluate
            python3 -u /starvers_eval/scripts/6_evaluate/query.py ${triple_store} "ostrich" ${dataset} ${ostrich_port}
            echo "$(log_timestamp) ${log_level}:Evaluation finished." >> $log_file

            # Stop memory tracker and database server
            echo "$(log_timestamp) ${log_level}:Kill memory tracker and process /opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js to shutdown Ostrich" >> $log_file
            kill "$mem_tracker_pid"
            pkill -f '/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js'
            while ps -ef | grep -q '[h]ttp.js'; do
                sleep 1
            done
            echo "$(log_timestamp) ${log_level}:/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js killed." >> $log_file
        done
    
    elif [ ${triple_store} == "jenatdb2" ]; then
        # Export variables
        export JAVA_HOME=/opt/java/java17/openjdk
        export PATH=/opt/java/java17/openjdk/bin:$PATH

        mkdir -p /run/configuration
        for policy in ${policies[@]}; do
            if ! policy_allowed ${triple_store} ${policy}; then
                echo "$(log_timestamp) ${log_level}:Policy ${policy} is not available for triplestore ${triple_store}, skipping..." >> $log_file
                continue
            fi

            for dataset in ${datasets[@]}; do

                # Start database server and run in background
                echo "$(log_timestamp) ${log_level}:Starting Fuseki server for the evaluation of ${policy}_${dataset}..." >> $log_file
                cp /starvers_eval/configs/ingest/jenatdb2/${policy}_${dataset}.ttl /run/configuration/config.ttl
                nohup /jena-fuseki/fuseki-server --port=3030 --tdb2 &

                # Wait until server is up
                echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
                counter=0
                while [[ $(curl -I http://Starvers:${jenatdb2_port} 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '200' ]]; do
                    sleep 1s
                    counter=$((counter + 1))
                    if [[ counter -ge 60 ]]; then
                        echo "$(log_timestamp) ${log_level}:Server not up after 60 seconds, restarting..." >> $log_file
                        pkill -f '/jena-fuseki/fuseki-server.jar'
                        nohup /jena-fuseki/fuseki-server --port=3030 --tdb2 &
                        counter=0
                    fi
                done
                echo "$(log_timestamp) ${log_level}:Fuseki server is up." >> $log_file

                # Start tracking memory consumption
                db_pid=$(pgrep -f '/jena-fuseki/fuseki-server.jar' | head -n 1)
                mem_tracker_pid=$(start_mem_tracker $db_pid "jenatdb2_${policy}_${dataset}" $mem_file 0.5)
                echo "$(log_timestamp) ${log_level}:Started memory tracker with PID ${mem_tracker_pid} for Jena TDB2." >> $log_file

                # Clean output directory
                rm -rf /starvers_eval/output/result_sets/${triple_store}/${policy}_${dataset}

                # Evaluate
                python3 -u /starvers_eval/scripts/6_evaluate/query.py ${triple_store} ${policy} ${dataset} ${jenatdb2_port}
                echo "$(log_timestamp) ${log_level}:Evaluation finished." >> $log_file

                # Stop memory tracker and database server
                echo "$(log_timestamp) ${log_level}:Kill memory tracker and process /jena-fuseki/fuseki-server.jar to shutdown Jena" >> $log_file
                kill "$mem_tracker_pid"
                pkill -f '/jena-fuseki/fuseki-server.jar'
                while ps -ef | grep -q '[j]ena-fuseki/fuseki-server.jar'; do
                    sleep 1
                done
                echo "$(log_timestamp) ${log_level}:/jena-fuseki/fuseki-server.jar killed." >> $log_file
            done
        done
    
    elif [ ${triple_store} == "graphdb" ]; then
        GDB_JAVA_OPTS_BASE=$GDB_JAVA_OPTS

        for policy in ${policies[@]}; do
            if ! policy_allowed ${triple_store} ${policy}; then
                echo "$(log_timestamp) ${log_level}:Policy ${policy} is not available for triplestore ${triple_store}, skipping..." >> $log_file
                continue
            fi

            for dataset in ${datasets[@]}; do
                # Export variables
                export JAVA_HOME=/opt/java/java11/openjdk
                export PATH=/opt/java/java11/openjdk/bin:$PATH
                export GDB_JAVA_OPTS="$GDB_JAVA_OPTS_BASE -Dgraphdb.home.data=/starvers_eval/databases/graphdb/${policy}_${dataset}"

                # Start database server and run in background
                echo "$(log_timestamp) ${log_level}:Starting GraphDB server for the evaluation of ${policy}_${dataset}..." >> $log_file
                /opt/graphdb/dist/bin/graphdb -d -s
                
                # Wait until server is up
                # GraphDB doesn't deliver HTTP code 200 for some reason ...
                echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
                while [[ $(curl -I http://Starvers:${graphdb_port} 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '406' ]]; do
                    sleep 1s
                done
                echo "$(log_timestamp) ${log_level}:GraphDB server is up" >> $log_file

                # Start tracking memory consumption
                db_pid=$(pgrep -f '${JAVA_HOME}/bin/java' | head -n 1)
                mem_tracker_pid=$(start_mem_tracker $db_pid "graphdb_${policy}_${dataset}" $mem_file 0.5)
                echo "$(log_timestamp) ${log_level}:Started memory tracker with PID ${mem_tracker_pid} for GraphDB." >> $log_file

                # Clean output directory
                rm -rf /starvers_eval/output/result_sets/${triple_store}/${policy}_${dataset}

                # Evaluate
                python3 -u /starvers_eval/scripts/6_evaluate/query.py ${triple_store} ${policy} ${dataset} ${graphdb_port}
                echo "$(log_timestamp) ${log_level}:Evaluation finished." >> $log_file

                # Stop memory tracker and database server                
                echo "$(log_timestamp) ${log_level}:Kill memory tracker and process ${JAVA_HOME}/bin/java to shutdown GraphDB" >> $log_file
                kill "$mem_tracker_pid"
                pkill -f ${JAVA_HOME}/bin/java
                while pgrep -f "${JAVA_HOME}/bin/java" >/dev/null; do
                    sleep 1
                done
                echo "$(log_timestamp) ${log_level}:${JAVA_HOME}/bin/java killed." >> $log_file

            done
        done
    fi
 
done
