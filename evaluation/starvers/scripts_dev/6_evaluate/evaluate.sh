#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/evaluate/query.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Bash arguments and environment variables
policies=("${policies}") 
datasets=("${datasets}") 
triple_stores=("${triple_stores}")
graphdb_port=$((7200))
jenatdb2_port=$((3030))

# Prepare directories and files
rm -rf /starvers_eval/output/logs/evaluate/
mkdir -p /starvers_eval/output/logs/evaluate/
> $log_file
> /starvers_eval/output/measurements/time.csv
echo "triplestore;dataset;policy;query_set;snapshot;snapshot_ts;query;execution_time;snapshot_creation_time" >> /starvers_eval/output/measurements/time.csv

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
    if [ ${triple_store} == "jenatdb2" ]; then
        # Set environment variables
        export JAVA_HOME=/opt/java/java17/openjdk
        export PATH=/opt/java/java17/openjdk/bin:$PATH

        mkdir -p /run/configuration
        for policy in ${policies[@]}; do
            for dataset in ${datasets[@]}; do
                # Export variables
                export JAVA_HOME=/usr/local/openjdk-11
                export PATH=/usr/local/openjdk-11/bin:$PATH

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

                # Clean output directory
                rm -rf /starvers_eval/output/result_sets/${triple_store}/${policy}_${dataset}

                # Evaluate
                /starvers_eval/python_venv/bin/python3 -u /starvers_eval/scripts/6_evaluate/query.py ${triple_store} ${policy} ${dataset} ${jenatdb2_port}

                # Stop database server
                echo "$(log_timestamp) ${log_level}:Kill process /jena-fuseki/fuseki-server.jar to shutdown Jena" >> $log_file
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

                # Clean output directory
                rm -rf /starvers_eval/output/result_sets/${triple_store}/${policy}_${dataset}

                # Evaluate
                python3 -u /starvers_eval/scripts/6_evaluate/query.py ${triple_store} ${policy} ${dataset} ${graphdb_port}

                # Stop database server
                echo "$(log_timestamp) ${log_level}:Kill process ${JAVA_HOME}/bin/java to shutdown GraphDB" >> $log_file
                pkill -f ${JAVA_HOME}/bin/java
                while pgrep -f "${JAVA_HOME}/bin/java" >/dev/null; do
                    sleep 1
                done
                echo "$(log_timestamp) ${log_level}:${JAVA_HOME}/bin/java killed." >> $log_file

            done
        done
    fi
 
done
