#!/bin/bash

# Logging variables
log_file=/starvers_eval/output/logs/download/downloads.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Prepare directories and files
rm -rf /starvers_eval/output/logs/download
mkdir -p /starvers_eval/output/logs/download
> $log_file

# Path variables
snapshot_dir=`grep -A 2 '[general]' /starvers_eval/configs/eval_setup.toml | awk -F '"' '/snapshot_dir/ {print $2}'`

# Other variables
datasets=("${datasets}") 
registered_datasets=$(grep -E '\[datasets\.[A-Za-z_]+\]' /starvers_eval/configs/eval_setup.toml | awk -F "." '{print $2}' | sed 's/.$//')
echo "$(log_timestamp) ${log_level}: Registered datasets are ${registered_datasets} ..." >> $log_file

######################################################
# Datasets
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

    echo "$(log_timestamp) ${log_level}: Downloading ${dataset} snapshots..." >> $log_file
    wget -t 3 -c -P /starvers_eval/rawdata/${dataset} ${download_link_snapshots}
    mkdir -p /starvers_eval/rawdata/${dataset}/${snapshot_dir}
    echo "$(log_timestamp) ${log_level}: Extracting ${dataset} snapshots..." >> $log_file
    tar -xf /starvers_eval/rawdata/${dataset}/${archive_name_snapshots} -C /starvers_eval/rawdata/${dataset}/${snapshot_dir}
    
    if [[ $yn_nested_archives == "true" ]]; then
        cd /starvers_eval/rawdata/${dataset}/${snapshot_dir}
        for f in *.gz ; do gzip -d < "$f" > /starvers_eval/rawdata/${dataset}/${snapshot_dir}/"${f%.*}" ; done
        rm *.gz
    fi
    
    echo "$(log_timestamp) ${log_level}: Downloading ${dataset} named graphs dataset..." >> $log_file
    wget -t 3 -c -P /starvers_eval/rawdata/${dataset} ${download_link_ng_dataset}
    echo "$(log_timestamp) ${log_level}: Extracting ${dataset} named graphs dataset..." >> $log_file
    gzip -d < /starvers_eval/rawdata/${dataset}/${archive_name_ng_dataset} > /starvers_eval/rawdata/${dataset}/alldata.TB.nq
    
    # for CB and CBNG policy: empty initial delete changeset
    > /starvers_eval/rawdata/${dataset}/empty.nt 
    
    echo "$(log_timestamp) ${log_level}: Downloading and extracting ${dataset} datasets finished." >> $log_file

done

######################################################
# Query sets
######################################################
raw_queries_path=/starvers_eval/queries/raw_queries
# Download BEAR query sets and extract queries

# Create directories
echo "$(log_timestamp) ${log_level}: Creating directories for queries" >> $log_file

mkdir -p ${raw_queries_path}/beara/low
mkdir -p ${raw_queries_path}/beara/high
mkdir -p ${raw_queries_path}/bearb/lookup
mkdir -p ${raw_queries_path}/bearb/join
mkdir -p ${raw_queries_path}/bearc/complex
mkdir -p ${raw_queries_path}/orkg/complex

# Download queries
echo "$(log_timestamp) ${log_level}: Downloading query sets for BEARA, BEARB, BEARC, and ORKG" >> $log_file

# BEARA low
wget -t 3 -c -P ${raw_queries_path}/beara/low https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/s/s-queries-lowCardinality.txt
wget -t 3 -c -P ${raw_queries_path}/beara/low https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/p/p-queries-lowCardinality.txt
wget -t 3 -c -P ${raw_queries_path}/beara/low https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/o/o-queries-lowCardinality.txt
wget -t 3 -c -P ${raw_queries_path}/beara/low https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/sp/sp-queries-lowCardinality.txt
wget -t 3 -c -P ${raw_queries_path}/beara/low https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/po/po-queries-lowCardinality.txt
wget -t 3 -c -P ${raw_queries_path}/beara/low https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/so/so-queries-lowCardinality.txt

# BEARA high
wget -t 3 -c -P ${raw_queries_path}/beara/high https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/s/s-queries-highCardinality.txt
wget -t 3 -c -P ${raw_queries_path}/beara/high https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/p/p-queries-highCardinality.txt  
wget -t 3 -c -P ${raw_queries_path}/beara/high https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/o/o-queries-highCardinality.txt
wget -t 3 -c -P ${raw_queries_path}/beara/high https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/sp/sp-queries-highCardinality.txt
wget -t 3 -c -P ${raw_queries_path}/beara/high https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/Queries/po/po-queries-highCardinality.txt

# BEARB Lookup
wget -t 3 -c -P ${raw_queries_path}/bearb/lookup https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/Queries/p/p.txt
wget -t 3 -c -P ${raw_queries_path}/bearb/lookup https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/Queries/po/po.txt

# BEARB join
wget -t 3 -c -P ${raw_queries_path}/bearb/join https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/Queries/joins.zip
unzip ${raw_queries_path}/bearb/join/joins.zip -d ${raw_queries_path}/bearb/join
rm ${raw_queries_path}/bearb/join/joins.zip

# BEARC complex
for ((i=1; i<=10; i++)); do
  wget -t 3 -c -P ${raw_queries_path}/bearc/complex "https://aic.ai.wu.ac.at/qadlod/bear/BEAR_C/Queries/q${i}.txt"
done

# SciQA complex
wget -t 3 -c -O ${raw_queries_path}/orkg/complex/SciQA-dataset.zip https://zenodo.org/records/7744048/files/SciQA-dataset.zip?download=1
unzip ${raw_queries_path}/orkg/complex/SciQA-dataset.zip -d ${raw_queries_path}/orkg/complex

echo "$(log_timestamp) ${log_level}: Finished downloading query sets and extracted them to ${raw_queries_path}" >> $log_file

