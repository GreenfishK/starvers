#!/bin/bash

# Functions

eval_combi_exists() {
    local triplestore="$1"
    local dataset="$2"
    local policy="$3"
    local config="/starvers_eval/configs/eval_setup.toml"

    # Extract the block [evaluations.<triplestore>] → next section
    local line
    line=$(awk -v store="$triplestore" -v ds="$dataset" '
        $0 ~ "^\\[evaluations\\."store"\\]" {found=1; next}
        found && /^\[/ {exit}
        found && $1 == ds {print; exit}
    ' "$config")

    # No dataset entry found
    [[ -z "$line" ]] && return 1

    # Check if policy is inside the array
    if echo "$line" | grep -qw "\"$policy\""; then
        return 0
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


# Logging variables
log_file=/starvers_eval/output/logs/evaluate/query.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Bash arguments and environment variables
policies=("${policies}") 
datasets=("${datasets}") 
triple_stores=("${triple_stores}")

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

    # Get triple store management script from eval_setup.toml
    triple_store_mgmt=$(python3 - <<EOF
import tomli
with open("$CONFIG", "rb") as f:
    data = tomli.load(f)
print(data["rdf_stores"]["$triple_store"]["mgmt_script"])
EOF
)

    for policy in ${policies[@]}; do

        # Check whether combination is supported
        if ! eval_combi_exists "${triple_store}" "${dataset}" "${policy}"; then
            echo "$(log_timestamp) ${log_level}:Policy ${policy} and dataset ${dataset} is not available for triplestore ${triple_store}, skipping..." >> $log_file
            continue
        fi

        for dataset in ${datasets[@]}; do
            # Start database server and run in background
            "${triple_store_mgmt}" startup $dataset &

            # Get PID
            pid_file="/tmp/${triple_store}_${policy}_${dataset}.pid"

            max_retries=3
            retry=1
            db_pid=""

            while [[ $retry -le $max_retries ]]; do
                sleep 3

                if [[ -f "$pid_file" ]]; then
                    db_pid=$(cat "$pid_file")
                    break
                fi

                echo "$(log_timestamp) ${log_level}:PID file not found (attempt $retry/$max_retries)" >> $log_file
                retry=$((retry + 1))
            done

            if [[ -z "$db_pid" ]]; then
                echo "$(log_timestamp) ${log_level}:ERROR: Could not obtain PID for ${triple_store} ${dataset}" >> $log_file
                exit 1
            fi

            # Start tracking memory consumption
            mem_tracker_pid=$(start_mem_tracker $db_pid "${policy}_${dataset}" $mem_file 0.5)
            echo "$(log_timestamp) ${log_level}:Started memory tracker with PID ${mem_tracker_pid} for Ostrich." >> $log_file

            # Clean output directory
            rm -rf /starvers_eval/output/result_sets/${triple_store}/${policy}_${dataset}

            # Evaluate
            python3 -u /starvers_eval/scripts/6_evaluate/query.py ${triple_store} ${triple_store} ${dataset}
            echo "$(log_timestamp) ${log_level}:Evaluation finished." >> $log_file

            # Stop memory tracker and database server
            echo "$(log_timestamp) ${log_level}:Kill memory tracker and process to shutdown ${triple_store}" >> $log_file
            kill "$mem_tracker_pid"
            "${triple_store_mgmt}" shutdown
        done
    done
done
