#!/bin/bash
# Input parametrers
policy=$1
dataset=$2

# Set variables
baseDir=/starvers_eval
script_dir=/starvers_eval/scripts
#jenatdb2_port=$((3030))
export JAVA_HOME=/usr/local/openjdk-11
export PATH=/usr/local/openjdk-11/bin:$PATH
export FUSEKI_HOME=/jena-fuseki
export #JAVA_OPTIONS="-Xmx90g -Xms90g"
export _JAVA_OPTIONS="-Xmx90g -Xms90g"
export ADMIN_PASSWORD=starvers

# Clean repository
rm -rf ${baseDir}/databases/preprocessing/jenatdb2_${policy}_${dataset}

# Create repositories
mkdir -p ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}

# Parametrize and copy config file
repositoryID=${policy}_${dataset}
cp ${script_dir}/2_preprocess/configs/jenatdb2-config_template.ttl ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
sed -i "s/{{repositoryID}}/$repositoryID/g" ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
sed -i "s/{{policy}}/$policy/g" ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
sed -i "s/{{dataset}}/$dataset/g" ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl

# Ingest empty dataset
/jena-fuseki/tdbloader2 --loc ${baseDir}/databases/preprocessing/jenatdb2_${policy}_${dataset}/${repositoryID} ${baseDir}/rawdata/${dataset}/empty.nt

# Start database server and run in background
cp ${baseDir}/configs/preprocessing/jenatdb2_${policy}_${dataset}/*.ttl /run/configuration
nohup /jena-fuseki/fuseki-server --port=3030 --tdb2 &

# Wait until server is up
echo "Waiting..."
while [[ $(curl -I http://Starvers:3030 2>/dev/null | head -n 1 | cut -d$' ' -f2) != '200' ]]; do
    sleep 1s
done
echo "Fuseki server is up"