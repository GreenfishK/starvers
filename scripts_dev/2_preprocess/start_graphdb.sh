#!/bin/bash
# Input parametrers
policy=$1
dataset=$2

# Set variables
script_dir=/starvers_eval/scripts
#graphdb_port=$((7200))
export JAVA_HOME=/opt/java/openjdk
export PATH=/opt/java/openjdk/bin:$PATH
export GDB_JAVA_OPTS="$GDB_JAVA_OPTS -Dgraphdb.home.data=/starvers_eval/databases/preprocessing/graphdb_${policy}_${dataset}/data"

# Clean repository
rm -rf /starvers_eval/databases/preprocessing/

# Create directories
mkdir -p /starvers_eval/configs/preprocessing/graphdb_${policy}_${dataset}

repositoryID=${policy}_${dataset}
cp ${script_dir}/2_preprocess/configs/graphdb-config_template.ttl ${script_dir}/2_preprocess/configs/graphdb-config.ttl
sed -i "s/{{repositoryID}}/$repositoryID/g" ${script_dir}/2_preprocess/configs/graphdb-config.ttl

# Ingest empty dataset
/opt/graphdb/dist/bin/preload -c ${script_dir}/2_preprocess/configs/graphdb-config.ttl /starvers_eval/rawdata/${dataset}/empty.nt --force

# Start database server and run in background
/opt/graphdb/dist/bin/graphdb -d -s

# Wait until server is up
# GraphDB doesn't deliver HTTP code 200 for some reason ...
echo "Waiting..."
while [[ $(curl -I http://Starvers:7200 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '406' ]]; do
    sleep 1s
done
echo "GraphDB server is up"