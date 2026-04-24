#!/bin/bash
# triple_store_mgmt/ostrich_aggchange_mgmt.sh
#
# Management script for the "ostrich_aggchange" policy.
#
# This is the Hose & Pelgrin adaptive-snapshot OSTRICH variant, ingested
# with the aggchange 2.0 strategy (best VM-query performer from the paper).
# At query time it exposes the same Comunica HTTP SPARQL endpoint on port
# 42564 as plain ostrich — the interface to evaluate.py is identical.
#
# The only difference from ostrich_mgmt.sh is the ingest command:
#   plain ostrich:       /opt/ostrich/ostrich-evaluate    ingest never     0    ...
#   ostrich_aggchange:   /opt/ostrich-hp/ostrich-evaluate ingest aggchange 2.0  ...
#
# Interface (mirrors ostrich_mgmt.sh exactly):
#   startup      <database_dir> <policy> <dataset>
#   shutdown
#   create_env   <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>
#   ingest       <database_dir> <dataset_dir_or_file> <policy> <dataset> <config_dir> [versions]
#   ingest_empty <database_dir> <policy> <dataset> <config_dir>
#   dump_repo    <database_dir> <policy> <dataset> <output_file>

# ─── Logging ────────────────────────────────────────────────────────────────
log_file=${RUN_DIR}/output/logs/ostrich_aggchange_mgmt.txt
log_dir=$(dirname "$log_file")
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

mkdir -p "$log_dir"
if [ ! -f "$log_file" ]; then
    > "$log_file"
fi

# ─── Functions ──────────────────────────────────────────────────────────────
startup() {
    echo "$(log_timestamp) ${log_level}:Start ostrich_aggchange (Comunica) node in background..." >> "$log_file"

    node /opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js \
        -p 42564 -h 0.0.0.0 -t 480 \
        ostrichFile@${database_dir} &

    echo "$(log_timestamp) ${log_level}:Waiting for endpoint..." >> "$log_file"
    until curl -s -X POST http://Starvers:42564 \
        -H "Content-Type: application/sparql-query" \
        --data "ASK {}" >/dev/null 2>&1
    do
        sleep 1
    done

    db_pid=$!
    echo $db_pid > /tmp/ostrich_aggchange_${policy}_${dataset}.pid
    echo "$(log_timestamp) ${log_level}:ostrich_aggchange node is up (PID $db_pid)" >> "$log_file"
}

shutdown() {
    echo "$(log_timestamp) ${log_level}:Shutdown ostrich_aggchange start" >> "$log_file"

    pidfile=$(ls /tmp/ostrich_aggchange_*.pid 2>/dev/null | head -n 1)

    if [ -z "$pidfile" ]; then
        echo "$(log_timestamp) ${log_level}:No PID file, fallback pkill" >> "$log_file"
        pkill -9 -f '/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js' 2>/dev/null || true
    else
        PID=$(cat "$pidfile")
        echo "$(log_timestamp) ${log_level}:PID file $pidfile → PID $PID" >> "$log_file"

        if ps -p "$PID" > /dev/null 2>&1; then
            kill -9 "$PID" 2>/dev/null || true
            for i in {1..10}; do
                ps -p "$PID" > /dev/null 2>&1 || break
                sleep 1
            done
        fi
        rm -f "$pidfile"
    fi

    for i in {1..30}; do
        ss -ltnp | grep -q ':42564' || break
        sleep 1
    done

    if ss -ltnp | grep -q ':42564'; then
        echo "$(log_timestamp) ${log_level}:WARNING — port 42564 still held after timeout" >> "$log_file"
    fi

    echo "$(log_timestamp) ${log_level}:Shutdown complete" >> "$log_file"
}

create_env() {
    if pgrep -f "/opt/comunica-feature-versioning/engines/query-sparql-ostrich/bin/http.js" >/dev/null; then
        shutdown
    fi

    echo "$(log_timestamp) ${log_level}:Clean database directory ${database_dir}" >> "$log_file"
    rm -rf "${database_dir}"
    rm -rf /tmp/ostrich_aggchange_*.pid

    mkdir -p "${database_dir}"

    # Same IC+CB virtual directory layout as ostrich_mgmt.sh
    raw_root="${RUN_DIR}/rawdata"
    vdir="${RUN_DIR}/rawdata/${dataset}/alldata_vdir"

    file_fmt_len=$(python3 - <<EOF
import tomli
with open("/starvers_eval/configs/eval_setup.toml","rb") as f:
    config = tomli.load(f)
print(config["datasets"]["$dataset"]["ic_basename_length"])
EOF
)
    first_snapshot=$(printf "%0${file_fmt_len}d.nt" 1)
    cb_src="${raw_root}/${dataset}/alldata.CB_computed.nt"
    ic_src="${raw_root}/${dataset}/alldata.IC.nt/${first_snapshot}"

    rm -rf "$vdir"
    mkdir -p "$vdir/alldata.IC.nt"
    cp "$ic_src"    "$vdir/alldata.IC.nt/$first_snapshot"
    cp -r "$cb_src" "$vdir/alldata.CB.nt"

    echo "$(log_timestamp) ${log_level}:Virtual directory ready at $vdir" >> "$log_file"
}

ingest() {
    if [[ -z "${versions:-}" ]]; then
        versions=$(python3 - <<EOF
import tomli
with open("/starvers_eval/configs/eval_setup.toml","rb") as f:
    config = tomli.load(f)
qs_name = list(config["datasets"]["$dataset"]["query_sets"].keys())[0]
print(config["datasets"]["$dataset"]["query_sets"][qs_name]["policies"]["$policy"]["versions"])
EOF
)
    fi

    echo "$(log_timestamp) ${log_level}:Ingest ${dataset}/${policy} with aggchange 2.0" >> "$log_file"

    # H&P fork CLI: ostrich-evaluate ingest <strategy> <param> <patches_dir> <start> <end>
    cd "${database_dir}" \
        && /opt/ostrich-hp/ostrich-evaluate ingest \
               aggchange 2.0 \
               "${dataset_dir_or_file}" \
               1 "${versions}"
}

ingest_empty() {
    echo "$(log_timestamp) ${log_level}:Ingest empty for ostrich_aggchange..." >> "$log_file"
    cd "${database_dir}" \
        && /opt/ostrich-hp/ostrich-evaluate ingest \
               never 0 \
               "${RUN_DIR}/rawdata/${dataset}/empty.nt" \
               1 1
}

dump_repo() {
    echo "$(log_timestamp) ${log_level}:Dump repository to ${output_file}..." >> "$log_file"
    curl -G "http://Starvers:42564/sparql" \
        --data-urlencode "query=CONSTRUCT { ?s ?p ?o } WHERE { GRAPH ?g { ?s ?p ?o } }" \
        -H "Accept: application/n-quads" \
        -o "${output_file}"
}

# ─── Argument dispatch ──────────────────────────────────────────────────────
set -euo pipefail

while [[ $# -gt 0 ]]; do
    case "$1" in
        --log-file) log_file="$2"; shift 2 ;;
        *) break ;;
    esac
done

if [[ ${1:-} == "startup" ]]; then
    [[ $# -ne 4 ]] && { echo "Usage: $0 startup <database_dir> <policy> <dataset>"; exit 1; }
    database_dir=$2; policy=$3; dataset=$4
    startup

elif [[ ${1:-} == "shutdown" ]]; then
    shutdown

elif [[ ${1:-} == "create_env" ]]; then
    [[ $# -ne 6 ]] && { echo "Usage: $0 create_env <policy> <dataset> <database_dir> <config_tmpl_dir> <config_dir>"; exit 1; }
    policy=$2; dataset=$3; database_dir=$4; config_tmpl_dir=$5; config_dir=$6
    create_env

elif [[ ${1:-} == "dump_repo" ]]; then
    [[ $# -ne 5 ]] && { echo "Usage: $0 dump_repo <database_dir> <policy> <dataset> <output_file>"; exit 1; }
    database_dir=$2; policy=$3; dataset=$4; output_file=$5
    dump_repo

elif [[ ${1:-} == "ingest_empty" ]]; then
    [[ $# -ne 5 ]] && { echo "Usage: $0 ingest_empty <database_dir> <policy> <dataset> <config_dir>"; exit 1; }
    database_dir=$2; policy=$3; dataset=$4; config_dir=$5
    ingest_empty

elif [[ ${1:-} == "ingest" ]]; then
    [[ $# -lt 6 || $# -gt 7 ]] && { echo "Usage: $0 ingest <database_dir> <dataset_dir_or_file> <policy> <dataset> <config_dir> [versions]"; exit 1; }
    database_dir=$2; dataset_dir_or_file=$3; policy=$4; dataset=$5; config_dir=$6; versions=${7:-}
    ingest

else
    echo "Usage: $0 startup|shutdown|create_env|ingest|ingest_empty|dump_repo ..."
    exit 1
fi
