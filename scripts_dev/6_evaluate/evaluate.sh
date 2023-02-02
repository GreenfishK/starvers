#!/bin/bash

policies=("${policies}") 
datasets=("${datasets}") 
triple_stores=("${triple_stores}")
graphdb_port=$((7200))
jenatdb2_port=$((3030))

# Init log and measurement files and setup logging
log_file=/starvers_eval/output/logs/evaluate/query.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"
mkdir -p /starvers_eval/output/logs/evaluate/
> $log_file
> /starvers_eval/output/measurements/time.csv

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
    mkdir -p /run/configuration
        for policy in ${policies[@]}; do
            for dataset in ${datasets[@]}; do
                # Export variables
                export JAVA_HOME=/usr/local/openjdk-11
                export PATH=/usr/local/openjdk-11/bin:$PATH

                # Start database server and run in background
                echo "$(log_timestamp) ${log_level}:Starting Fuseki server for the evaluation of ${policy}_${dataset}..." >> $log_file
                cp /starvers_eval/configs/jenatdb2/${policy}_${dataset}.ttl /run/configuration/config.ttl
                nohup /jena-fuseki/fuseki-server --port=3030 --tdb2 &

                # Wait until server is up
                echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
                while [[ $(curl -I http://Starvers:${jenatdb2_port} 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '200' ]]; do
                    sleep 1s
                done
                echo "$(log_timestamp) ${log_level}:Fuseki server is up." >> $log_file

                # Clean output directory
                rm -rf /starvers_eval/output/result_sets/${triple_store}/${policy}_${dataset}

                # Evaluate
                /starvers_eval/python_venv/bin/python3 -u /starvers_eval/scripts/6_evaluate/query.py ${triple_store} ${policy} ${dataset} ${jenatdb2_port}

                # Stop database server
                echo "$(log_timestamp) ${log_level}:Shutting down fuseki server and finishing evaluation of ${policy}_${dataset}." >> $log_file
                pkill -f '/jena-fuseki/fuseki-server.jar'
                
            done
        done
    
    GDB_JAVA_OPTS_BASE=$GDB_JAVA_OPTS
    elif [ ${triple_store} == "graphdb" ]; then
        for policy in ${policies[@]}; do
            for dataset in ${datasets[@]}; do
                # Export variables
                export JAVA_HOME=/opt/java/openjdk
                export PATH=/opt/java/openjdk/bin:$PATH
                export GDB_JAVA_OPTS="$GDB_JAVA_OPTS_BASE -Dgraphdb.home.data=/starvers_eval/databases/graphdb/${policy}_${dataset}"

                # Start database server and run in background
                echo "$(log_timestamp) ${log_level}:Starting GraphDB server..." >> $log_file
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
                /starvers_eval/python_venv/bin/python3 -u /starvers_eval/scripts/6_evaluate/query.py ${triple_store} ${policy} ${dataset} ${graphdb_port}

                # Stop database server
                echo "$(log_timestamp) ${log_level}:Shutting down GraphDB server." >> $log_file
                pkill -f '/opt/java/openjdk/bin/java'

            done
        done
    fi
 
done
