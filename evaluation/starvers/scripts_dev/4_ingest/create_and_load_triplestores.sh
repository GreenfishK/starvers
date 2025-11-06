#!/bin/bash

# Logging variables
ingest_logs=/starvers_eval/output/logs/ingest
log_file_graphdb=${ingest_logs}/ingestion_graphdb.txt
log_file_jena=${ingest_logs}/ingestion_jena.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Bash arguments and environment variables
datasets=("${datasets}") 
triple_stores=("${triple_stores}")
policies=("${policies}") 

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

if [[ " ${triple_stores[*]} " =~ " graphdb " ]]; then
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
        case $policy in 
            ic_mr_tr) datasetDirOrFile=${snapshot_dir};;
            cb_mr_tr) datasetDirOrFile=${change_sets_dir};;
            ic_sr_ng) datasetDirOrFile=alldata.ICNG.trig;;
            cb_sr_ng) datasetDirOrFile=alldata.CBNG.trig;;
            tb_sr_ng) datasetDirOrFile=alldata.TB.nq;;
            tb_sr_rs) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
            *)
                echo "Policy must be in ic_mr_tr, cb_mr_tr, ic_sr_ng, cb_sr_ng, tb_sr_ng, tb_sr_rs" >> $log_file_graphdb
                exit 2
            ;;
        esac

        for dataset in ${datasets[@]}; do
            export GDB_JAVA_OPTS="$GDB_JAVA_OPTS_BASE -Dgraphdb.home.data=/starvers_eval/databases/graphdb/${policy}_${dataset}"
            versions=`get_snapshot_version "${dataset}"`
            file_name_struc=`get_snapshot_filename_struc "${dataset}"`


            for run in {1..10}; do
                echo "$(log_timestamp) ${log_level}:Process is $policy, $dataset for GraphDB; run: $run" >> $log_file_graphdb
                total_ingestion_time=0
                total_file_size=0
                if [[ "$policy" == "tb_sr_rs" || "$policy" == "tb_sr_ng" || "$policy" == "ic_sr_ng" || "$policy" == "cb_sr_ng" ]]; then
                    # Replace repositoryID in config template
                    repositoryID=${policy}_${dataset}
                    cp ${script_dir}/4_ingest/configs/graphdb-config_template.ttl $configs_dir/${repositoryID}.ttl
                    sed -i "s/{{repositoryID}}/$repositoryID/g" $configs_dir/${repositoryID}.ttl

                    # Load data into GraphDB
                    ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c $configs_dir/${repositoryID}.ttl /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}) \
                                    2>&1 1>> $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                    total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                    file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${datasetDirOrFile} | awk '{print substr($5, 1, length($5)-1)}'`
                    total_file_size=`echo "$total_file_size + $file_size/1024" | bc` 

                elif [ "$policy" == "ic_mr_tr" ]; then
                    for c in $(seq -f $file_name_struc 1 ${versions}) # ${versions}
                    do
                        # Replace repositoryID in config template
                        repositoryID=${policy}_${dataset}_$((10#$c))
                        cp ${script_dir}/4_ingest/configs/graphdb-config_template.ttl $configs_dir/${repositoryID}.ttl
                        sed -i "s/{{repositoryID}}/$repositoryID/g"  $configs_dir/${repositoryID}.ttl

                        # Load data into GraphDB
                        ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c $configs_dir/${repositoryID}.ttl /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt) \
                                        2>&1 1>> $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                        total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                        file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt  | awk '{print substr($5, 1, length($5)-1)}'`
                        total_file_size=`echo "$total_file_size + $file_size/1024" | bc`
                    done  
                            
                elif [ "$policy" == "cb_mr_tr" ]; then
                    for v in $(seq 0 1 $((${versions}-1))); do 
                        ve=$(echo $v+1 | bc)
                        if [ $v -eq 0 ]; then
                            file_name=`printf "$file_name_struc" +1`.nt
                            fileadd="$snapshot_dir/$file_name"
                            filedel="empty.nt"
                            repositoryIDAdd=${policy}_${dataset}_ic1
                            repositoryIDDel=${policy}_${dataset}_empty
                        else
                            fileadd="${datasetDirOrFile}/data-added_$v-$ve.ttl"
                            filedel="${datasetDirOrFile}/data-deleted_$v-$ve.ttl"
                            repositoryIDAdd=${policy}_${dataset}_add_$v-$ve
                            repositoryIDDel=${policy}_${dataset}_del_$v-$ve
                        fi

                        # Add
                        # Replace repositoryID in config template
                        cp ${script_dir}/4_ingest/configs/graphdb-config_template.ttl $configs_dir/${repositoryIDAdd}.ttl
                        sed -i "s/{{repositoryID}}/$repositoryIDAdd/g" $configs_dir/${repositoryIDAdd}.ttl

                        # Load data into GraphDB
                        ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c $configs_dir/${repositoryIDAdd}.ttl /starvers_eval/rawdata/${dataset}/${fileadd}) \
                                        2>&1 1>> $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                        total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                        file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${fileadd} | awk '{print substr($5, 1, length($5)-1)}'`
                        total_file_size=`echo "$total_file_size + $file_size/1024" | bc`

                        # Delete
                        # Replace repositoryID in config template
                        cp ${script_dir}/4_ingest/configs/graphdb-config_template.ttl $configs_dir/${repositoryIDDel}.ttl
                        sed -i "s/{{repositoryID}}/$repositoryIDDel/g" $configs_dir/${repositoryIDDel}.ttl

                        # Load data into GraphDB
                        ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c $configs_dir/${repositoryIDDel}.ttl /starvers_eval/rawdata/${dataset}/${filedel}) \
                                        2>&1 1>> $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                        total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                        file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${filedel} | awk '{print substr($5, 1, length($5)-1)}'`
                        total_file_size=`echo "$total_file_size + $file_size/1024" | bc`
                    done
                fi

                echo "$(log_timestamp) ${log_level}:Kill process /opt/java/openjdk/bin/java to shutdown GraphDB" >> $log_file_graphdb
                pkill -f /opt/java/openjdk/bin/java

                cat $log_file_graphdb | grep -v "\[.*\] DEBUG"
                disk_usage=`du -s --block-size=M --apparent-size $db_dir/${policy}_${dataset}/repositories | awk '{print substr($1, 1, length($1)-1)}'`
                echo "graphdb;${policy};${dataset};${run};${total_ingestion_time};${total_file_size};${disk_usage}" >> $measurements
            done
        done
    done
fi

if [[ " ${triple_stores[*]} " =~ " jenatdb2 " ]]; then
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
        case $policy in 
            ic_mr_tr) datasetDirOrFile=${snapshot_dir};;
            cb_mr_tr) datasetDirOrFile=${change_sets_dir};;
            ic_sr_ng) datasetDirOrFile=alldata.ICNG.trig;;
            cb_sr_ng) datasetDirOrFile=alldata.CBNG.trig;;
            tb_sr_ng) datasetDirOrFile=alldata.TB.nq;;
            tb_sr_rs) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
            *)
                echo "Policy must be in ic_mr_tr, cb_mr_tr, ic_sr_ng, cb_sr_ng, tb_sr_ng, tb_sr_rs" >> $log_file_jena
                exit 2
            ;;
        esac

        for dataset in ${datasets[@]}; do
            # Set variables
            data_dir=$db_dir/${policy}_${dataset}
            versions=`get_snapshot_version "${dataset}"`
            file_name_struc=`get_snapshot_filename_struc "${dataset}"`

            for run in {1..10}; do
                echo "$(log_timestamp) ${log_level}:Process is $policy, $dataset for JenaTDB2; run: $run" >> $log_file_jena
                total_ingestion_time=0
                total_file_size=0            
                if [[ "$policy" == "tb_sr_rs" || "$policy" == "tb_sr_ng" || "$policy" == "ic_sr_ng" || "$policy" == "cb_sr_ng" ]]; then
                    repositoryID=${policy}_${dataset}
                    # Replace repositoryID in config template
                    
                    cp ${script_dir}/4_ingest/configs/jenatdb2-config_template.ttl $configs_dir/${repositoryID}.ttl
                    sed -i "s/{{repositoryID}}/$repositoryID/g" $configs_dir/${repositoryID}.ttl
                    sed -i "s/{{policy}}/$policy/g" $configs_dir/${repositoryID}.ttl
                    sed -i "s/{{dataset}}/$dataset/g" $configs_dir/${repositoryID}.ttl
                    
                    # Load data into Jena
                    echo "$(log_timestamp) ${log_level}:Loading $dataset into JenaTDB2." >> $log_file_jena
                    mkdir -p "$data_dir/${repositoryID}"
                    ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc $data_dir/${repositoryID} /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}) \
                                    2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                    total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                    echo "$(log_timestamp) ${log_level}:Done loading." >> $log_file_jena

                    file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${datasetDirOrFile} | awk '{print substr($5, 1, length($5)-1)}'`
                    total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  
                    echo "$(log_timestamp) ${log_level}:Total file size is: $total_file_size MiB" >> $log_file_jena
           
                elif [ "$policy" == "ic_mr_tr" ]; then
                    for c in $(seq -f $file_name_struc 1 ${versions})
                    do
                        repositoryID=${policy}_${dataset}_$((10#$c))
                        # Replace repositoryID in config template
                        cp ${script_dir}/4_ingest/configs/jenatdb2-config_template.ttl $configs_dir/${repositoryID}.ttl
                        sed -i "s/{{repositoryID}}/$repositoryID/g" $configs_dir/${repositoryID}.ttl
                        sed -i "s/{{policy}}/$policy/g" $configs_dir/${repositoryID}.ttl
                        sed -i "s/{{dataset}}/$dataset/g" $configs_dir/${repositoryID}.ttl
                        
                        # Load data into Jena
                        echo "$(log_timestamp) ${log_level}:Loading $dataset into JenaTDB2." >> $log_file_jena
                        mkdir -p "$data_dir/${repositoryID}"
                        ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc $data_dir/${repositoryID} /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt) \
                                        2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                        total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                        echo "$(log_timestamp) ${log_level}:Done loading." >> $log_file_jena

                        file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt | awk '{print substr($5, 1, length($5)-1)}'`
                        total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  
                        echo "$(log_timestamp) ${log_level}:Total file size is: $total_file_size MiB" >> $log_file_jena

                    done
                
                elif [ "$policy" == "cb_mr_tr" ]; then
                    for v in $(seq 0 1 $((${versions}-1))); do 
                        ve=$(echo $v+1 | bc)
                        if [ $v -eq 0 ]; then
                            file_name=`printf "$file_name_struc" +1`.nt
                            fileadd="$snapshot_dir/$file_name"
                            filedel="empty.nt"
                            repositoryIDAdd=${policy}_${dataset}_ic1
                            repositoryIDDel=${policy}_${dataset}_empty
                        else
                            fileadd="${datasetDirOrFile}/data-added_$v-$ve.ttl"
                            filedel="${datasetDirOrFile}/data-deleted_$v-$ve.ttl"
                            repositoryIDAdd=${policy}_${dataset}_add_$v-$ve
                            repositoryIDDel=${policy}_${dataset}_del_$v-$ve
                        fi

                        # Replace repositoryID in config template
                        cp ${script_dir}/4_ingest/configs/jenatdb2-config_template.ttl $configs_dir/${repositoryIDAdd}.ttl
                        sed -i "s/{{repositoryID}}/$repositoryIDAdd/g" $configs_dir/${repositoryIDAdd}.ttl
                        sed -i "s/{{policy}}/$policy/g" $configs_dir/${repositoryIDAdd}.ttl
                        sed -i "s/{{dataset}}/$dataset/g" $configs_dir/${repositoryIDAdd}.ttl

                        # Load data into Jena TDB2
                        echo "$(log_timestamp) ${log_level}:Loading $dataset into JenaTDB2." >> $log_file_jena
                        mkdir -p "$data_dir/${repositoryID}"
                        ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc $data_dir/${repositoryIDAdd} /starvers_eval/rawdata/${dataset}/${fileadd}) \
                                        2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                        total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                        echo "$(log_timestamp) ${log_level}:Done loading." >> $log_file_jena
                        
                        file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${fileadd} | awk '{print substr($5, 1, length($5)-1)}'`
                        total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  
                        echo "$(log_timestamp) ${log_level}:Total file siz is: $total_file_size MiB" >> $log_file_jena

                        # Replace repositoryID in config template
                        cp ${script_dir}/4_ingest/configs/jenatdb2-config_template.ttl $configs_dir/${repositoryIDDel}.ttl
                        sed -i "s/{{repositoryID}}/$repositoryIDDel/g" $configs_dir/${repositoryIDDel}.ttl
                        sed -i "s/{{policy}}/$policy/g" $configs_dir/${repositoryIDDel}.ttl
                        sed -i "s/{{dataset}}/$dataset/g" $configs_dir/${repositoryIDDel}.ttl

                        # Load data into Jena TDB2
                        echo "$(log_timestamp) ${log_level}:Loading $dataset into JenaTDB2." >> $log_file_jena
                        mkdir -p "$data_dir/${repositoryID}"
                        ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc $data_dir/${repositoryIDDel} /starvers_eval/rawdata/${dataset}/${filedel}) \
                                        2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                        total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                        echo "$(log_timestamp) ${log_level}:Done loading." >> $log_file_jena
                        
                        file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${filedel} | awk '{print substr($5, 1, length($5)-1)}'`
                        total_file_size=`echo "$total_file_size + $file_size/1024" | bc`    
                        echo "$(log_timestamp) ${log_level}:Total file size is: $total_file_size MB" >> $log_file_jena
           
                    done
                fi
                echo "$(log_timestamp) ${log_level}:Kill process /jena-fuseki/fuseki-server.jar to shutdown Jena" >> $log_file_jena
                pkill -f '/jena-fuseki/fuseki-server.jar'

                cat $log_file_jena | grep -v "\[.*\] DEBUG"
                disk_usage=`du -s --block-size=M --apparent-size $data_dir | awk '{print substr($1, 1, length($1)-1)}'`
                echo "$(log_timestamp) ${log_level}:DB file size is: $disk_usage MB" >> $log_file_jena
                echo "jenatdb2;${policy};${dataset};${run};${total_ingestion_time};${total_file_size};${disk_usage}" >> $measurements
            done
        done
    done
fi


