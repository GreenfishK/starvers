#!/bin/bash

# Set variables
SCRIPT_DIR=/starvers_eval/scripts
datasets=("${datasets}") 
triple_stores=("${triple_stores}")
policies=("${policies}") 
measurements=/starvers_eval/output/measurements/ingestion.csv
log_file_graphdb=/starvers_eval/output/logs/ingest/ingestion_graphdb.txt
log_file_jena=/starvers_eval/output/logs/ingest/ingestion_jena.txt
log_timestamp() { date +%Y-%m-%d\ %A\ %H:%M:%S; }
log_level="root:INFO"

# Create directories
mkdir -p /starvers_eval/output/logs/ingest/

echo "triple_store;policy;dataset;ingestion_time;raw_file_size_MiB;db_files_disk_usage_MiB" > $measurements

if [[ " ${triple_stores[*]} " =~ " graphdb " ]]; then
    export JAVA_HOME=/opt/java/openjdk
    export PATH=/opt/java/openjdk/bin:$PATH
    graphdb_evns=$GDB_JAVA_OPTS
    > $log_file_graphdb

    for policy in ${policies[@]}; do
        case $policy in 
            ic_mr_tr) datasetDirOrFile=alldata.IC.nt;;
            cb_mr_tr) datasetDirOrFile=alldata.CB_computed.nt;;
            ic_sr_ng) datasetDirOrFile=alldata.ICNG.trig;;
            cb_sr_ng) datasetDirOrFile=alldata.CBNG.trig;;
            tb_sr_ng) datasetDirOrFile=alldata.TB.nq;;
            tb_sr_rs) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
            *)
                echo "Policy must be in ic_mr_tr, cb_mr_tr, ic_sr_ng, cb_sr_ng, tb_sr_ng, tb_sr_rs"
                exit 2
            ;;
        esac

        for dataset in ${datasets[@]}; do
            # Clean database directory
            rm -rf /starvers_eval/databases/graphdb_${policy}_${dataset}

            case $dataset in 
                beara) versions=58 file_name_struc="%01g";;
                bearb_hour) versions=1299 file_name_struc="%06g";; 
                bearb_day) versions=89 file_name_struc="%06g";;
                bearc) versions=33 file_name_struc="%01g";;
                *)
                    echo "graphdb: Dataset must be in beara bearb_hour bearb_day bearc"
                    exit 2
                ;;
            esac
            export GDB_JAVA_OPTS="$graphdb_evns -Dgraphdb.home.data=/starvers_eval/databases/graphdb_${policy}_${dataset}/data"
            
            echo "$(log_timestamp) ${log_level}:Process is $policy, $dataset for GraphDB"
            total_ingestion_time=0
            total_file_size=0
            if [[ "$policy" == "tb_sr_rs" || "$policy" == "tb_sr_ng" || "$policy" == "ic_sr_ng" || "$policy" == "cb_sr_ng" ]]; then
                # Replace repositoryID in config template
                repositoryID=${policy}_${dataset}
                cp ${SCRIPT_DIR}/2_preprocess/configs/graphdb-config_template.ttl /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryID}.ttl

                # Load data into GraphDB
                ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryID}.ttl /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}) \
                                2>&1 1>> $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >>  $log_file_graphdb
                file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${datasetDirOrFile} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc` 

            elif [ "$policy" == "ic_mr_tr" ]; then
                for c in $(seq -f $file_name_struc 1 ${versions}) # ${versions}
                do
                    # Replace repositoryID in config template
                    repositoryID=${policy}_${dataset}_$((10#$c))
                    cp ${SCRIPT_DIR}/2_preprocess/configs/graphdb-config_template.ttl /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryID}.ttl
                    sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryID}.ttl

                    # Load data into GraphDB
                    ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryID}.ttl /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt) \
                                    2>&1 1>>  $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                    total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                    echo "\n\n" >>  $log_file_graphdb
                    file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt  | awk '{print substr($5, 1, length($5)-1)}'`
                    total_file_size=`echo "$total_file_size + $file_size/1024" | bc`
                done  
                        
            elif [ "$policy" == "cb_mr_tr" ]; then
                for v in $(seq 0 1 $((${versions}-1))); do 
                    ve=$(echo $v+1 | bc)
                    if [ $v -eq 0 ]; then
                        file_name=`printf "$file_name_struc" +1`.nt
                        fileadd="alldata.IC.nt/$file_name"
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
                    cp ${SCRIPT_DIR}/2_preprocess/configs/graphdb-config_template.ttl /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryIDAdd}.ttl
                    sed -i "s/{{repositoryID}}/$repositoryIDAdd/g" /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryIDAdd}.ttl

                    # Load data into GraphDB
                    ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryIDAdd}.ttl /starvers_eval/rawdata/${dataset}/${fileadd}) \
                                    2>&1 1>> $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                    total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                    echo "\n\n" >> $log_file_graphdb
                    file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${fileadd} | awk '{print substr($5, 1, length($5)-1)}'`
                    total_file_size=`echo "$total_file_size + $file_size/1024" | bc`

                    # Delete
                    # Replace repositoryID in config template
                    cp ${SCRIPT_DIR}/2_preprocess/configs/graphdb-config_template.ttl /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryIDDel}.ttl
                    sed -i "s/{{repositoryID}}/$repositoryIDDel/g" /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryIDDel}.ttl

                    # Load data into GraphDB
                    ingestion_time=`(time -p /opt/graphdb/dist/bin/importrdf preload --force -c /starvers_eval/configs/graphdb_${policy}_${dataset}/${repositoryIDDel}.ttl /starvers_eval/rawdata/${dataset}/${filedel}) \
                                    2>&1 1>> $log_file_graphdb | grep -oP "real \K.*" | sed "s/,/./g" `
                    total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                    echo "\n\n" >> $log_file_graphdb     
                    file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${filedel} | awk '{print substr($5, 1, length($5)-1)}'`
                    total_file_size=`echo "$total_file_size + $file_size/1024" | bc`
                done
            fi

            echo "$(log_timestamp) ${log_level}:Kill process /opt/java/openjdk/bin/java to shutdown GraphDB" >> $log_file_graphdb
            pkill -f /opt/java/openjdk/bin/java

            cat /starvers_eval/output/logs/ingest/ingestion_graphdb_logs.txt | grep -v "\[.*\] DEBUG"
            disk_usage=`du -s --block-size=M --apparent-size /starvers_eval/databases/graphdb_${policy}_${dataset}/data/repositories | awk '{print substr($1, 1, length($1)-1)}'`
            echo "GraphDB;${policy};${dataset};${total_ingestion_time};${total_file_size};${disk_usage}" >> $measurements
        done
    done
fi

if [[ " ${triple_stores[*]} " =~ " jenatdb2 " ]]; then
    export JAVA_HOME=/usr/local/openjdk-11
    export PATH=/usr/local/openjdk-11/bin:$PATH
    > $log_file_jena
    rm -rf /starvers_eval/configs/*

    for policy in ${policies[@]}; do
        case $policy in 
            ic_mr_tr) datasetDirOrFile=alldata.IC.nt;;
            cb_mr_tr) datasetDirOrFile=alldata.CB_computed.nt;;
            ic_sr_ng) datasetDirOrFile=alldata.ICNG.trig;;
            cb_sr_ng) datasetDirOrFile=alldata.CBNG.trig;;
            tb_sr_ng) datasetDirOrFile=alldata.TB.nq;;
            tb_sr_rs) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
            *)
                echo "Policy must be in ic_mr_tr, cb_mr_tr, ic_sr_ng, cb_sr_ng, tb_sr_ng, tb_sr_rs"
                exit 2
            ;;
        esac

        for dataset in ${datasets[@]}; do
            # Clean database directory
            rm -rf /starvers_eval/databases/jenatdb2_${policy}_${dataset}

            case $dataset in 
                beara) versions=58 file_name_struc="%01g";;
                bearb_hour) versions=1299 file_name_struc="%06g";; 
                bearb_day) versions=89 file_name_struc="%06g";;
                bearc) versions=33 file_name_struc="%01g";;
                *)
                    echo "jenatdb2: Dataset must be in beara bearb_hour bearb_day bearc"
                    exit 2
                ;;
            esac

            echo "$(log_timestamp) ${log_level}:Process is $policy, $dataset for JenaTDB2"
            total_ingestion_time=0
            total_file_size=0
            mkdir -p /starvers_eval/configs/jenatdb2_${policy}_${dataset}
            
            if [[ "$policy" == "tb_sr_rs" || "$policy" == "tb_sr_ng" || "$policy" == "ic_sr_ng" || "$policy" == "cb_sr_ng" ]]; then
                repositoryID=${policy}_${dataset}
                # Replace repositoryID in config template
                
                cp ${SCRIPT_DIR}/2_preprocess/configs/jenatdb2-config_template.ttl /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{policy}}/$policy/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{dataset}}/$dataset/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                
                # Load data into Jena
                ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc /starvers_eval/databases/jenatdb2_${policy}_${dataset}/${repositoryID} /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}) \
                                2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $log_file_jena
                file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${datasetDirOrFile} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`             

            elif [ "$policy" == "ic_mr_tr" ]; then
                for c in $(seq -f $file_name_struc 1 ${versions})
                do
                    repositoryID=${policy}_${dataset}_$((10#$c))
                    # Replace repositoryID in config template
                    cp ${SCRIPT_DIR}/2_preprocess/configs/jenatdb2-config_template.ttl /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                    sed -i "s/{{repositoryID}}/$repositoryID/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                    sed -i "s/{{policy}}/$policy/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                    sed -i "s/{{dataset}}/$dataset/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                    
                    # Load data into Jena
                    ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc /starvers_eval/databases/jenatdb2_${policy}_${dataset}/${repositoryID} /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt) \
                                    2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                    total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                    echo "\n\n" >> $log_file_jena
                    file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt | awk '{print substr($5, 1, length($5)-1)}'`
                    total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  
                done
            
            elif [ "$policy" == "cb_mr_tr" ]; then
                for v in $(seq 0 1 $((${versions}-1))); do 
                    ve=$(echo $v+1 | bc)
                    if [ $v -eq 0 ]; then
                        file_name=`printf "$file_name_struc" +1`.nt
                        fileadd="alldata.IC.nt/$file_name"
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
                    cp ${SCRIPT_DIR}/2_preprocess/configs/jenatdb2-config_template.ttl /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryIDAdd}.ttl
                    sed -i "s/{{repositoryID}}/$repositoryIDAdd/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryIDAdd}.ttl
                    sed -i "s/{{policy}}/$policy/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryIDAdd}.ttl
                    sed -i "s/{{dataset}}/$dataset/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryIDAdd}.ttl

                    # Load data into Jena TDB2
                    ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc /starvers_eval/databases/jenatdb2_${policy}_${dataset}/${repositoryIDAdd} /starvers_eval/rawdata/${dataset}/${fileadd}) \
                                    2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                    total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                    echo "\n\n" >> $log_file_jena
                    file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${fileadd} | awk '{print substr($5, 1, length($5)-1)}'`
                    total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  

                    # Replace repositoryID in config template
                    cp ${SCRIPT_DIR}/2_preprocess/configs/jenatdb2-config_template.ttl /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryIDDel}.ttl
                    sed -i "s/{{repositoryID}}/$repositoryIDDel/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryIDDel}.ttl
                    sed -i "s/{{policy}}/$policy/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryIDDel}.ttl
                    sed -i "s/{{dataset}}/$dataset/g" /starvers_eval/configs/jenatdb2_${policy}_${dataset}/${repositoryIDDel}.ttl

                    # Load data into Jena TDB2
                    ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc /starvers_eval/databases/jenatdb2_${policy}_${dataset}/${repositoryIDDel} /starvers_eval/rawdata/${dataset}/${filedel}) \
                                    2>&1 1>> $log_file_jena | grep -oP "real \K.*" | sed "s/,/./g" `
                    total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                    echo "\n\n" >> $log_file_jena 
                    file_size=`ls -l --block-size=k /starvers_eval/rawdata/${dataset}/${filedel} | awk '{print substr($5, 1, length($5)-1)}'`
                    total_file_size=`echo "$total_file_size + $file_size/1024" | bc`               
                done
            fi
            cat $log_file_jena | grep -v "\[.*\] DEBUG"
            disk_usage=`du -s --block-size=M --apparent-size /starvers_eval/databases/jenatdb2_${policy}_${dataset} | awk '{print substr($1, 1, length($1)-1)}'`
            echo "JenaTDB2;${policy};${dataset};${total_ingestion_time};${total_file_size};${disk_usage}" >> $measurements
        done
    done
fi


