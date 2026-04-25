#!/bin/bash

# Logging variables
log_file=$RUN_DIR/output/logs/download/downloads.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Prepare directories and files
rm -rf $RUN_DIR/output/logs/download
mkdir -p $RUN_DIR/output/logs/download
> $log_file

metadata_file=$RUN_DIR/output/logs/download/datasets_meta.csv
queries_file=$RUN_DIR/output/logs/download/queries_meta.csv
> $metadata_file
> $queries_file
echo "dataset,snapshot_dir,size" >> $metadata_file
echo "query_set,for_dataset,count,links" >> $queries_file

# Path variables
snapshot_dir=`grep -A 2 '[general]' /starvers_eval/configs/eval_setup.toml | awk -F '"' '/snapshot_dir/ {print $2}'`

# Other variables
datasets=("${datasets}") 
registered_datasets=$(grep -E '\[datasets\.[A-Za-z_]+\]' /starvers_eval/configs/eval_setup.toml | awk -F "." '{print $2}' | sed 's/.$//')
echo "$(log_timestamp) ${log_level}: Registered datasets are ${registered_datasets} ..." >> $log_file

######################################################
# Helper: read a TOML array from [query_sets.<name>]
# Returns newline-separated list of URLs
######################################################
# Read download_links for a query set from datasets.<dataset>.query_sets.<qs>
# Usage: read_qs_links <dataset> <qs_name>
read_qs_links() {
    local dataset=$1
    local qs_name=$2
    python3 - "$dataset" "$qs_name" /starvers_eval/configs/eval_setup.toml <<'EOF'
import sys, tomli
dataset, qs_name = sys.argv[1], sys.argv[2]
with open(sys.argv[3], "rb") as f:
    config = tomli.load(f)
links = config["datasets"][dataset]["query_sets"][qs_name].get("download_links", [])
print("\n".join(links))
EOF
}

# Read a scalar field from datasets.<dataset>.query_sets.<qs>
# Usage: read_qs_field <dataset> <qs_name> <field>
read_qs_field() {
    local dataset=$1
    local qs_name=$2
    local field=$3
    python3 - "$dataset" "$qs_name" "$field" /starvers_eval/configs/eval_setup.toml <<'EOF'
import sys, tomli
dataset, qs_name, field = sys.argv[1], sys.argv[2], sys.argv[3]
with open(sys.argv[4], "rb") as f:
    config = tomli.load(f)
print(config["datasets"][dataset]["query_sets"][qs_name].get(field, ""))
EOF
}

######################################################
# Helper: count queries for a downloaded query set
# Usage: count_queries <dir> <count_method>
#   count_method = "lines"  → sum non-empty lines across all .txt files
#   count_method = "files"  → count files in dir (excluding zips)
######################################################
count_queries() {
    local dir=$1
    local method=$2
    if [[ "$method" == "lines" ]]; then
        # Sum non-empty lines across all .txt files
        find "$dir" -maxdepth 1 -name "*.txt" | xargs -r grep -c "" | \
            awk -F: '{sum += $NF} END {print sum+0}'
    else
        # Count files (non-zip, non-directory)
        find "$dir" -maxdepth 2 -type f ! -name "*.zip" | wc -l | tr -d ' '
    fi
}

######################################################
# Helper: record a query set row into queries_meta.csv
# Usage: record_query_set <qs_name> <dir>
######################################################
record_query_set() {
    local dataset=$1
    local qs_name=$2
    local dir=$3

    local for_dataset="$dataset"
    local count_method
    count_method=$(read_qs_field "$dataset" "$qs_name" "count_method")
    local count
    count=$(count_queries "$dir" "$count_method")

    # Links: one row per file in the dir, with filename and original URL
    # Build "filename; url" pairs, semicolon-separated between pairs, pipe-separated between files
    local links_str=""
    while IFS= read -r url; do
        [[ -z "$url" ]] && continue
        local fname
        fname=$(basename "$url" | sed 's/?.*$//')    # strip query params
        if [[ -n "$links_str" ]]; then
            links_str="${links_str} | ${fname}; ${url}"
        else
            links_str="${fname}; ${url}"
        fi
    done < <(read_qs_links "$dataset" "$qs_name")

    echo "${qs_name},${for_dataset},${count},${links_str}" >> $queries_file
    echo "$(log_timestamp) ${log_level}: Recorded query set ${qs_name}: count=${count}" >> $log_file
}

######################################################
# Datasets: BEAR and ORKG
######################################################
for dataset in ${datasets[@]}; do

    if ! [[ ${registered_datasets[@]} =~ $dataset ]]; then
        echo "$(log_timestamp) ${log_level}: Dataset ${dataset} is not within the registered datasets: ${registered_datasets} ..." >> $log_file
        continue
    fi

    download_link_snapshots=`grep -A 8 -E "\[datasets\.${dataset}\]" /starvers_eval/configs/eval_setup.toml | grep 'download_link_snapshots' | awk '{print $3}' | sed 's/"//g'`
    archive_name_snapshots=`grep -A 8 -E "\[datasets\.${dataset}\]" /starvers_eval/configs/eval_setup.toml | grep 'archive_name_snapshots' | awk '{print $3}' | sed 's/"//g'`
    download_link_ng_dataset=`grep -A 8 -E "\[datasets\.${dataset}\]" /starvers_eval/configs/eval_setup.toml | grep 'download_link_ng_dataset' | awk '{print $3}' | sed 's/"//g'`
    archive_name_ng_dataset=`grep -A 8 -E "\[datasets\.${dataset}\]" /starvers_eval/configs/eval_setup.toml | grep 'archive_name_ng_dataset' | awk '{print $3}' | sed 's/"//g'`
    yn_nested_archives=`grep -A 8 -E "\[datasets\.${dataset}\]" /starvers_eval/configs/eval_setup.toml | grep 'yn_nested_archives' | awk '{print $3}' | sed 's/"//g'`

    # tar
    echo "$(log_timestamp) ${log_level}: Downloading ${dataset} snapshots..." >> $log_file
    wget -t 3 -c -P $RUN_DIR/rawdata/${dataset} ${download_link_snapshots}
    mkdir -p $RUN_DIR/rawdata/${dataset}/${snapshot_dir}
    echo "$(log_timestamp) ${log_level}: Extracting ${dataset} snapshots..." >> $log_file
    tar -xf $RUN_DIR/rawdata/${dataset}/${archive_name_snapshots} -C $RUN_DIR/rawdata/${dataset}/${snapshot_dir}

    # Record size of extracted snapshots and save to RUN_DIR/output/logs/downloads/datasets_meta.csv
    size=$(du -s -L --block-size=1M --apparent-size $RUN_DIR/rawdata/${dataset}/${snapshot_dir} | cut -f1)
    echo "${dataset},${snapshot_dir},${size}" >> $metadata_file
    if [[ $yn_nested_archives == "true" ]]; then
        cd $RUN_DIR/rawdata/${dataset}/${snapshot_dir}
        for f in *.gz ; do gzip -d < "$f" > $RUN_DIR/rawdata/${dataset}/${snapshot_dir}/"${f%.*}" ; done
        rm *.gz
    fi
    
    # gz
    echo "$(log_timestamp) ${log_level}: Downloading ${dataset} named graphs dataset..." >> $log_file
    wget -t 3 -c -P $RUN_DIR/rawdata/${dataset} ${download_link_ng_dataset}
    echo "$(log_timestamp) ${log_level}: Extracting ${dataset} named graphs dataset..." >> $log_file
    gzip -d < $RUN_DIR/rawdata/${dataset}/${archive_name_ng_dataset} > $RUN_DIR/rawdata/${dataset}/alldata.TB.nq
    
    # for CB and CBNG policy: empty initial delete changeset
    # For filtering ORKG queries. Send them against an empty repository
    > $RUN_DIR/rawdata/${dataset}/empty.nt 
    
    echo "$(log_timestamp) ${log_level}: Downloading and extracting ${dataset} datasets finished." >> $log_file

done



######################################################
# Query sets
######################################################
raw_queries_path=$RUN_DIR/queries/raw_queries

# Create directories
echo "$(log_timestamp) ${log_level}: Creating directories for queries" >> $log_file

mkdir -p ${raw_queries_path}/beara/low
mkdir -p ${raw_queries_path}/beara/high
mkdir -p ${raw_queries_path}/bearb/lookup
mkdir -p ${raw_queries_path}/bearb/join
mkdir -p ${raw_queries_path}/bearc/complex
mkdir -p ${raw_queries_path}/orkg/complex

echo "$(log_timestamp) ${log_level}: Downloading query sets for BEARA, BEARB, BEARC, and ORKG" >> $log_file

# ── BEARA low ────────────────────────────────────────────────────────────────
while IFS= read -r url; do
    [[ -z "$url" ]] && continue
    wget -t 3 -c -P ${raw_queries_path}/beara/low "$url"
done < <(read_qs_links "beara" "low")
record_query_set "beara" "low" "${raw_queries_path}/beara/low"

# ── BEARA high ───────────────────────────────────────────────────────────────
while IFS= read -r url; do
    [[ -z "$url" ]] && continue
    wget -t 3 -c -P ${raw_queries_path}/beara/high "$url"
done < <(read_qs_links "beara" "high")
record_query_set "beara" "high" "${raw_queries_path}/beara/high"

# ── BEARB lookup ─────────────────────────────────────────────────────────────
while IFS= read -r url; do
    [[ -z "$url" ]] && continue
    wget -t 3 -c -P ${raw_queries_path}/bearb/lookup "$url"
done < <(read_qs_links "bearb" "lookup")
record_query_set "bearb" "lookup" "${raw_queries_path}/bearb/lookup"

# ── BEARB join ───────────────────────────────────────────────────────────────
while IFS= read -r url; do
    [[ -z "$url" ]] && continue
    wget -t 3 -c -P ${raw_queries_path}/bearb/join "$url"
done < <(read_qs_links "bearb" "join")
unzip -o ${raw_queries_path}/bearb/join/joins.zip -d ${raw_queries_path}/bearb/join
rm ${raw_queries_path}/bearb/join/joins.zip
record_query_set "bearb" "join" "${raw_queries_path}/bearb/join"

# ── BEARC complex ────────────────────────────────────────────────────────────
while IFS= read -r url; do
    [[ -z "$url" ]] && continue
    wget -t 3 -c -P ${raw_queries_path}/bearc/complex "$url"
done < <(read_qs_links "bearc" "complex")
record_query_set "bearc" "complex" "${raw_queries_path}/bearc/complex"

# ── ORKG complex ─────────────────────────────────────────────────────────────
while IFS= read -r url; do
    [[ -z "$url" ]] && continue
    wget -t 3 -c -O ${raw_queries_path}/orkg/complex/SciQA-dataset.zip "$url"
done < <(read_qs_links "orkg" "complex")
unzip -o ${raw_queries_path}/orkg/complex/SciQA-dataset.zip -d ${raw_queries_path}/orkg/complex
record_query_set "orkg" "complex" "${raw_queries_path}/orkg/complex"

echo "$(log_timestamp) ${log_level}: Finished downloading query sets and extracted them to ${raw_queries_path}" >> $log_file