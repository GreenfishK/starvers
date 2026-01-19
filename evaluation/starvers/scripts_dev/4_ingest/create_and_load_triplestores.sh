#!/bin/bash

# Logging variables
ingest_logs=/starvers_eval/output/logs/ingest
log_file_graphdb=${ingest_logs}/ingestion_graphdb.txt
log_file_jena=${ingest_logs}/ingestion_jena.txt
log_file_ostrich=${ingest_logs}/ingestion_ostrich.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Bash arguments and environment variables
datasets=("${datasets}") 
triple_stores=("${triple_stores}")
policies=("${policies}") 

# Validate policies
for policy in ${policies[@]}; do
    if [[ ! " ostrich ic_sr_ng cb_sr_ng tb_sr_ng tb_sr_rs " =~ " ${policy} " ]]; then
        echo "$(log_timestamp) ${log_level}:Policy must be in: ostrich, ic_sr_ng, cb_sr_ng, tb_sr_ng, tb_sr_rs" >> $log_file_ostrich
        exit 2
    fi
done

runs=10

# Prepare directories and files
measurements=/starvers_eval/output/measurements/ingestion.csv
echo "triplestore;policy;dataset;run;ingestion_time;raw_file_size_MiB;db_files_disk_usage_MiB" > $measurements
mkdir -p $ingest_logs

# Path variables
script_dir=/starvers_eval/scripts
snapshot_dir=`grep -A 2 '[general]' /starvers_eval/configs/eval_setup.toml | awk -F '"' '/snapshot_dir/ {print $2}'`
change_sets_dir=`grep -A 2 '[general]' /starvers_eval/configs/eval_setup.toml | awk -F '"' '/change_sets_dir/ {print $2}'`

# Functions
get_snapshot_version() {
  result=`grep -A 2 "\[datasets\.$1\]" /starvers_eval/configs/eval_setup.toml | grep -E '^\s*snapshot_versions\s*=' | awk '{print $3}'`
  if [ -z "$result" ]; then
    echo "$(log_timestamp) ${log_level}:graphdb: Dataset must be in one of the datasets configured in the eval_setup.toml" >> $log_file
    return 2
  else
    echo "$result"
  fi
}

get_snapshot_filename_struc() { 
  snapshot_filename_struc=`grep -A 2 "\[datasets\.$1\]" /starvers_eval/configs/eval_setup.toml | grep -E '^\s*ic_basename_length\s*=' | awk '{print $3}'`
  if [ -z "$snapshot_filename_struc" ]; then
    echo "Error: snapshot filename structure returned empty." >&2
    return 2
  fi
  echo "%0${snapshot_filename_struc}g";
}

# Create a datasetDirOrFile - policy map
declare -A datasetDirOrFile_map
datasetDirOrFile_map["ostrich"]="/starvers_eval/rawdata" # Ostrich cb-based approach
datasetDirOrFile_map["ic_sr_ng"]="alldata.ICNG.trig" # my ic-based approach
datasetDirOrFile_map["cb_sr_ng"]="alldata.CBNG.trig" # my cb-based approach
datasetDirOrFile_map["tb_sr_ng"]="alldata.TB.nq" # bear
datasetDirOrFile_map["tb_sr_rs"]="alldata.TB_star_hierarchical.ttl" # rdf-star

if [[ " ${triple_stores[*]} " =~ " ostrich " ]]; then
    echo "$(log_timestamp) ${log_level}:Starting ingestion for Ostrich." >> $log_file_ostrich

    log_file=$log_file_ostrich

    # Path variables
    db_dir=/starvers_eval/databases/ostrich

    # Prepare directories and files
    rm -rf $db_dir
    mkdir -p $db_dir
    > $log_file_ostrich

    for policy in ${policies[@]}; do
        datasetDirOrFile=${datasetDirOrFile_map[$policy]}

        # if policy not one of: ostrich: skip loop
        if [[ ! " ostrich " =~ " ${policy} " ]]; then
            echo "$(log_timestamp) ${log_level}:Skipping policy $policy for Ostrich." >> $log_file_ostrich
            continue
        fi

        for dataset in ${datasets[@]}; do
            versions=`get_snapshot_version "${dataset}" "{log_file}"`
            file_name_struc=`get_snapshot_filename_struc "${dataset}"`

            # Create a virtual directory and put the file /starvers_eval/rawdata/${dataset}/alldata.IC.nt/000001.nt and all files from /starvers_eval/rawdata/${dataset}/alldata.CB_computed.nt into a virtual directory with magic links
            ostrich_virtual_dir=/ostrich/${dataset}
            rm -rf $ostrich_virtual_dir/alldata.IC.nt
            mkdir -p $ostrich_virtual_dir/alldata.IC.nt

            # Create symbolic links to the data files
            ln -s ${datasetDirOrFile}/${dataset}/alldata.CB_computed.nt $ostrich_virtual_dir/alldata.CB.nt
            ln -s ${datasetDirOrFile}/${dataset}/alldata.IC.nt/`printf "$file_name_struc" 1`.nt $ostrich_virtual_dir/alldata.IC.nt/`printf "$file_name_struc" 1`.nt
            echo "$(log_timestamp) ${log_level}:Created virtual directory for Ostrich at $ostrich_virtual_dir" >> $log_file_ostrich

            for ((run=1; run<=runs; run++)); do
                echo "$(log_timestamp) ${log_level}:Process is $policy, $dataset for Ostrich; run: $run" >> $log_file_ostrich
                total_ingestion_time=0
                total_file_size=0

                # Create and change to database directory
                mkdir ${db_dir}/${policy}_${dataset}
                cd ${db_dir}/${policy}_${dataset}

                # Measure ingestion time
                ingestion_time=`(time -p /opt/ostrich/ostrich-evaluate ingest never 0 ${ostrich_virtual_dir} 1 ${versions}) \
                                    2>&1 1>> $log_file_ostrich | grep -oP "real \K.*" | sed "s/,/./g" `
                echo "$(log_timestamp) ${log_level}:${ingestion_time}." >> $log_file_ostrich
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                
                # Measure file sizes
                cb_size=$(du -s --block-size=1M --apparent-size \
                    "${datasetDirOrFile}/${dataset}/alldata.CB_computed.nt" | awk '{print $1}')

                ic_size=$(du -s --block-size=1M --apparent-size \
                    "${datasetDirOrFile}/${dataset}/alldata.IC.nt/$(printf "$file_name_struc" 1).nt" | awk '{print $1}')

                total_file_size=$((cb_size + ic_size))

                # Save measurements
                disk_usage=`du -s --block-size=M $db_dir/${policy}_${dataset} | awk '{print $1}'`
                echo "ostrich;${policy};${dataset};${run};${total_ingestion_time};${total_file_size};${disk_usage}" >> $measurements

                # Cleanup
                if [[ $run -lt $runs ]]; then
                    echo "$(log_timestamp) ${log_level}:Cleaning up database for next run." >> $log_file_graphdb
                    rm -rf $db_dir/${policy}_${dataset}
                fi
            
            done
        done
    done

fi

if [[ " ${triple_stores[*]} " =~ " graphdb " ]]; then
    echo "$(log_timestamp) ${log_level}:Starting ingestion for GraphDB." >> $log_file_graphdb

    log_file=$log_file_graphdb

    # Bash arguments and environment variables
    export JAVA_HOME=/opt/java/java11/openjdk
    export PATH=/opt/java/java11/openjdk/bin:$PATH
    GDB_JAVA_OPTS_BASE=$GDB_JAVA_OPTS

    # Path variables
    configs_dir=/starvers_eval/configs/ingest/graphdb
    db_dir=/starvers_eval/databases/graphdb

    # Prepare directories and files
    rm -rf $configs_dir
    rm -rf $db_dir
    mkdir -p $configs_dir
    mkdir -p $db_dir
    > $log_file_graphdb


    for policy in ${policies[@]}; do
        datasetDirOrFile=${datasetDirOrFile_map[$policy]}

        # if policy not one of: ic_sr_ng cb_sr_ng tb_sr_ng tb_sr_rs: skip loop
        if [[ ! " ic_sr_ng cb_sr_ng tb_sr_ng tb_sr_rs " =~ " ${policy} " ]]; then
            echo "$(log_timestamp) ${log_level}:Skipping policy $policy for GraphDB." >> $log_file_graphdb
            continue
        fi

        for dataset in ${datasets[@]}; do
            export GDB_JAVA_OPTS="$GDB_JAVA_OPTS_BASE -Dgraphdb.home.data=/starvers_eval/databases/graphdb/${policy}_${dataset}"
            versions=`get_snapshot_version "${dataset}" "{log_file}"`
            file_name_struc=`get_snapshot_filename_struc "${dataset}"`

            for ((run=1; run<=runs; run++)); do
                echo "$(log_timestamp) ${log_level}:Process is $policy, $dataset for GraphDB; run: $run" >> $log_file_graphdb
                total_ingestion_time=0
                total_file_size=0

                # Create and change to database directory
                mkdir ${db_dir}/${policy}_${dataset}
                cd ${db_dir}/${policy}_${dataset}

                # Replace repositoryID in config template
                repositoryID=${policy}_${dataset}
                cp ${script_dir}/4_ingest/configs/graphdb-config_template.ttl $configs_dir/${repositoryID}.ttl
                sed -i "s/{{repositoryID}}/$repositoryID/g" $configs_dir/${repositoryID}.ttl

                # Load data into GraphDB. $db_dir/${repositoryID} gets created automatically
                ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c $configs_dir/${repositoryID}.ttl /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}) \
                                2>&1 1>> $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                
                # Measure file sizes
                total_file_size=$(du -s --block-size=1M --apparent-size "/starvers_eval/rawdata/${dataset}/${datasetDirOrFile}" | awk '{print $1}')

                # Shutdown GraphDB
                echo "$(log_timestamp) ${log_level}:Kill process /opt/java/openjdk/bin/java to shutdown GraphDB" >> $log_file_graphdb
                pkill -f /opt/java/openjdk/bin/java

                # Save measurements
                cat $log_file_graphdb | grep -v "\[.*\] DEBUG"
                disk_usage=`du -s --block-size=M $db_dir/${policy}_${dataset}/repositories | awk '{print $1}'`
                echo "graphdb;${policy};${dataset};${run};${total_ingestion_time};${total_file_size};${disk_usage}" >> $measurements

                # Cleanup
                if [[ $run -lt $runs ]]; then
                    echo "$(log_timestamp) ${log_level}:Cleaning up database for next run." >> $log_file_graphdb
                    rm -rf $db_dir/${repositoryID}
                fi
            done
        done
    done
    echo "$(log_timestamp) ${log_level}:Finished ingestion for GraphDB." >> $log_file_graphdb
fi

if [[ " ${triple_stores[*]} " =~ " jenatdb2 " ]]; then
    echo "$(log_timestamp) ${log_level}:Starting ingestion for JenaTDB2." >> $log_file_jena

    log_file=$log_file_jena
    
    # Bash arguments and environment variables
    export JAVA_HOME=/opt/java/java17/openjdk
    export PATH=/opt/java/java17/openjdk/bin:$PATH

    # Path variables
    configs_dir=/starvers_eval/configs/ingest/jenatdb2
    db_dir=/starvers_eval/databases/jenatdb2

    # Prepare directories and files
    rm -rf $db_dir
    rm -rf $configs_dir
    mkdir -p $db_dir
    mkdir -p $configs_dir
    > $log_file_jena


    for policy in ${policies[@]}; do
        datasetDirOrFile=${datasetDirOrFile_map[$policy]}

        # if policy not one of: ic_sr_ng cb_sr_ng tb_sr_ng tb_sr_rs: skip loop
        if [[ ! " ic_sr_ng cb_sr_ng tb_sr_ng tb_sr_rs " =~ " ${policy} " ]]; then
            echo "$(log_timestamp) ${log_level}:Skipping policy $policy for JenaTDB2." >> $log_file_jena
            continue
        fi

        for dataset in ${datasets[@]}; do
            # Set variables
            data_dir=$db_dir/${policy}_${dataset}
            versions=`get_snapshot_version "${dataset}" "{log_file}"`
            file_name_struc=`get_snapshot_filename_struc "${dataset}"`

            for ((run=1; run<=runs; run++)); do
                echo "$(log_timestamp) ${log_level}:Process is $policy, $dataset for JenaTDB2; run: $run" >> $log_file_jena
                total_ingestion_time=0
                total_file_size=0  

                repositoryID=${policy}_${dataset}
                # Replace repositoryID in config template
                
                cp ${script_dir}/4_ingest/configs/jenatdb2-config_template.ttl $configs_dir/${repositoryID}.ttl
                sed -i "s/{{repositoryID}}/$repositoryID/g" $configs_dir/${repositoryID}.ttl
                sed -i "s/{{policy}}/$policy/g" $configs_dir/${repositoryID}.ttl
                sed -i "s/{{dataset}}/$dataset/g" $configs_dir/${repositoryID}.ttl
                
                # Load data into Jena. $db_dir/${repositoryID} gets created automatically
                echo "$(log_timestamp) ${log_level}:Loading $dataset into JenaTDB2." >> $log_file_jena
                mkdir -p "$data_dir/${repositoryID}"
                ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc $data_dir/${repositoryID} /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}) \
                                2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "$(log_timestamp) ${log_level}:Done loading." >> $log_file_jena

                # Measure file sizes
                total_file_size=$(du -s --block-size=1M --apparent-size "/starvers_eval/rawdata/${dataset}/${datasetDirOrFile}" | awk '{print $1}')

                # Shutdown Jena Fuseki
                echo "$(log_timestamp) ${log_level}:Kill process /jena-fuseki/fuseki-server.jar to shutdown Jena" >> $log_file_jena
                pkill -f '/jena-fuseki/fuseki-server.jar'

                # Save measurements
                cat $log_file_jena | grep -v "\[.*\] DEBUG"
                disk_usage=`du -s --block-size=M $data_dir | awk '{print $1}'`
                echo "jenatdb2;${policy};${dataset};${run};${total_ingestion_time};${total_file_size};${disk_usage}" >> $measurements

                # Cleanup
                if [[ $run -lt $runs ]]; then
                    echo "$(log_timestamp) ${log_level}:Cleaning up database for next run." >> $log_file_graphdb
                    rm -rf $db_dir/${repositoryID}
                fi    
            done
        done
    done
    echo "$(log_timestamp) ${log_level}:Finished ingestion for JenaTDB2." >> $log_file_jena
fi


