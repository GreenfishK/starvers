#!/bin/bash
# Input parametrers
policy=$1
dataset=$2
ingest=$3
reset=$4

# Set variables
script_dir=/starvers_eval/scripts
log_file=/starvers_eval/output/logs/preprocessing/construct_datasets.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

#jenatdb2_port=$((3030))
export JAVA_HOME=/usr/local/openjdk-11
export PATH=/usr/local/openjdk-11/bin:$PATH

if [ $reset == "true" ]; then
    echo "$(log_timestamp) ${log_level}:Clean repositories..." >> $log_file
    rm -rf /starvers_eval/databases/preprocessing/*
    rm -rf /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}
    rm -rf /run/configuration

    echo "$(log_timestamp) ${log_level}:Create directories..." >> $log_file
    mkdir -p /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}
    mkdir -p /run/configuration

    echo "$(log_timestamp) ${log_level}:Parametrize and copy config file..." >> $log_file
    repositoryID=${policy}_${dataset}
    cp ${script_dir}/2_preprocess/configs/jenatdb2-config_template.ttl /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
    sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
    sed -i "s/{{policy}}/$policy/g" /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
    sed -i "s/{{dataset}}/$dataset/g" /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
fi

if [ $ingest == "true" ]; then
    echo "$(log_timestamp) ${log_level}:Ingest empty dataset..." >> $log_file
    /jena-fuseki/tdbloader2 --loc /starvers_eval/databases/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID} /starvers_eval/rawdata/${dataset}/empty.nt
fi

echo "$(log_timestamp) ${log_level}:Start database server in background..." >> $log_file
cp /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/*.ttl /run/configuration
nohup /jena-fuseki/fuseki-server --port=3030 --tdb2 &

# Wait until server is up
echo "$(log_timestamp) ${log_level}:Waiting..." >> $log_file
while [[ $(curl -I http://Starvers:3030 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '200' ]]; do
    sleep 1s
done
echo "$(log_timestamp) ${log_level}:Fuseki server is up" >> $log_file