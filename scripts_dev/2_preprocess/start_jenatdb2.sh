#!/bin/bash
# Input parametrers
policy=$1
dataset=$2
ingest=$3
reset=$4

# Set variables
script_dir=/starvers_eval/scripts
#jenatdb2_port=$((3030))
export JAVA_HOME=/usr/local/openjdk-11
export PATH=/usr/local/openjdk-11/bin:$PATH
#export FUSEKI_HOME=/jena-fuseki
#export _JAVA_OPTIONS="-Xmx90g -Xms90g"
#export ADMIN_PASSWORD=starvers

if [ $reset == "true" ]; then
    echo "Clean repositories..."
    rm -rf /starvers_eval/databases/preprocessing/
    rm -rf /run/configuration

    echo "Create directories..."
    mkdir -p /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}
    mkdir -p /run/configuration
fi

# Parametrize and copy config file
repositoryID=${policy}_${dataset}
cp ${script_dir}/2_preprocess/configs/jenatdb2-config_template.ttl /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
sed -i "s/{{policy}}/$policy/g" /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
sed -i "s/{{dataset}}/$dataset/g" /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl

if [ $ingest == "true" ]; then
    echo "Ingest empty dataset..."
    /jena-fuseki/tdbloader2 --loc /starvers_eval/databases/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID} /starvers_eval/rawdata/${dataset}/empty.nt
fi

# Start database server and run in background
cp /starvers_eval/configs/preprocessing/jenatdb2_${policy}_${dataset}/*.ttl /run/configuration
nohup /jena-fuseki/fuseki-server --port=3030 --tdb2 &

# Wait until server is up
echo "Waiting..."
while [[ $(curl -I http://Starvers:3030 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '200' ]]; do
    sleep 1s
done
echo "Fuseki server is up"