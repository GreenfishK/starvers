#!/bin/bash

# Variables
baseDir=~/.BEAR
configFile=graphdb-config.ttl
policies="ic" # cb tbsf tbsh tb
datasets="bearb-hour" # bearb-day beara bearc
current_time=`date "+%Y-%m-%dT%H:%M:%S"`
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

mkdir -p $baseDir/output/measurements/${current_time}
mkdir -p $baseDir/output/logs/${current_time}
echo "triple_store;policy;dataset;ingestion_time;raw_file_size_MiB;db_files_disk_usage_MiB" >> $baseDir/output/measurements/${current_time}/ingestion.txt  


### GraphDB ##################################################################

for policy in ${policies[@]}; do
    case $policy in 
        ic) datasetDirOrFile=alldata.IC.nt;;
        cb) datasetDirOrFile=alldata.CB_computed.nt;;
        tb) datasetDirOrFile=alldata.TB.nq;;
        tbsf) datasetDirOrFile=alldata.TB_star_flat.ttl;;
        tbsh) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
        *)
            echo "Policy must be in ic, cb, tb, tbsf, tbsh"
            exit 2
        ;;
    esac

    for dataset in ${datasets[@]}; do
        case $dataset in 
            beara) versions=58;;
            bearb-hour) versions=1299;;
            bearb-day) versions=89;;
            bearc) versions=32;;
            *)
                echo "Dataset must be in beara bearb-hour bearb-day bearc"
                exit 2
            ;;
        esac
        
        echo "Process is $policy, $dataset for GraphDB"
        total_ingestion_time=0
        total_file_size=0
        if [[ "$policy" == "tbsh" || "$policy" == "tbsf" || "$policy" == "tb" ]]; then
            # Replace repositoryID in config template
            repositoryID=${policy}_${dataset}
            cp ${SCRIPT_DIR}/configs/graphdb-config_template.ttl ${SCRIPT_DIR}/configs/graphdb-config.ttl
            sed -i "s/{{repositoryID}}/$repositoryID/g" ${SCRIPT_DIR}/configs/graphdb-config.ttl

            # Load data into GraphDB
            ingestion_time=`(policy=${policy} dataset=${dataset} rel_path_import_file=${datasetDirOrFile} \
                            time -p docker-compose run --rm graphdb_load) \
                            2>&1 1>> $baseDir/output/logs/${current_time}/graphDB_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
            total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
            echo "\n\n" >> $baseDir/output/logs/${current_time}/graphDB_logs.txt
            file_size=`ls -l --block-size=k ~/.BEAR/rawdata/${dataset}/${datasetDirOrFile} | awk '{print substr($5, 1, length($5)-1)}'`
            total_file_size=`echo "$total_file_size + $file_size/1024" | bc` 

        elif [ "$policy" == "ic" ]; then
            for c in $(seq -f "%06g" 1 1) # ${versions}
            do
                # Replace repositoryID in config template
                repositoryID=${policy}_${dataset}_$((10#$c))
                cp ${SCRIPT_DIR}/configs/graphdb-config_template.ttl ${SCRIPT_DIR}/configs/graphdb-config.ttl
                sed -i "s/{{repositoryID}}/$repositoryID/g" ${SCRIPT_DIR}/configs/graphdb-config.ttl

                # Load data into GraphDB
                ingestion_time=`(policy=${policy} dataset=${dataset} rel_path_import_file=${datasetDirOrFile}/${c}.nt \
                                time -p docker-compose run --rm graphdb_load) \
                                2>&1 1>> $baseDir/output/logs/${current_time}/graphDB_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/${current_time}/graphDB_logs.txt
                file_size=`ls -l --block-size=k ~/.BEAR/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt  | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`
            done
        
        elif [ "$policy" == "cb" ]; then
            for v in $(seq 0 1 2); do #${versions} -1
                ve=$(echo $v+1 | bc)
                if [ $v -eq 0 ]; then
                    fileadd="alldata.IC.nt/000001.nt"
                    filedel="empty.nt"
                    repositoryIDAdd=${policy}_${dataset}_ic1
                    repositoryIDDel=${policy}_${dataset}_empty
                else
                    fileadd="${datasetDirOrFile}/data-added_$v-$ve.nt"
                    filedel="${datasetDirOrFile}/data-deleted_$v-$ve.nt"
                    repositoryIDAdd=${policy}_${dataset}_add_$v-$ve
                    repositoryIDDel=${policy}_${dataset}_del_$v-$ve
                fi

                # Add
                # Replace repositoryID in config template
                cp ${SCRIPT_DIR}/configs/graphdb-config_template.ttl ${SCRIPT_DIR}/configs/graphdb-config.ttl
                sed -i "s/{{repositoryID}}/$repositoryIDAdd/g" ${SCRIPT_DIR}/configs/graphdb-config.ttl

                # Load data into GraphDB
                ingestion_time=`(policy=${policy} dataset=${dataset} rel_path_import_file=${fileadd} \
                                time -p docker-compose run --rm graphdb_load) \
                                2>&1 1>> $baseDir/output/logs/${current_time}/graphDB_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/${current_time}/graphDB_logs.txt
                file_size=`ls -l --block-size=k ~/.BEAR/rawdata/${dataset}/${fileadd} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`

                # Delete
                # Replace repositoryID in config template
                cp ${SCRIPT_DIR}/configs/graphdb-config_template.ttl ${SCRIPT_DIR}/configs/graphdb-config.ttl
                sed -i "s/{{repositoryID}}/$repositoryIDDel/g" ${SCRIPT_DIR}/configs/graphdb-config.ttl

                # Load data into GraphDB
                ingestion_time=`(policy=${policy} dataset=${dataset} rel_path_import_file=${filedel} \
                                time -p docker-compose run --rm graphdb_load) \
                                2>&1 1>> $baseDir/output/logs/${current_time}/graphDB_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/${current_time}/graphDB_logs.txt     
                file_size=`ls -l --block-size=k ~/.BEAR/rawdata/${dataset}/${filedel} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`
            done
        fi
        cat $baseDir/output/logs/${current_time}/graphDB_logs.txt | grep -v "\[.*\] DEBUG"
        disk_usage=`du -s --block-size=M --apparent-size ~/.BEAR/databases/graphdb_${policy}_${dataset}/data/repositories | awk '{print substr($1, 1, length($1)-1)}'`
        echo "GraphDB;${policy};${dataset};${total_ingestion_time};${total_file_size};${disk_usage}" >> $baseDir/output/measurements/${current_time}/ingestion.txt  
    done
done


### JenaTDB2 #################################################################

for policy in ${policies[@]}; do
    case $policy in 
        ic) datasetDirOrFile=alldata.IC.nt;;
        cb) datasetDirOrFile=alldata.CB_computed.nt;;
        tb) datasetDirOrFile=alldata.TB.nq;;
        tbsf) datasetDirOrFile=alldata.TB_star_flat.ttl;;
        tbsh) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
        *)
            echo "Policy must be in ic, cb, tb, tbsf, tbsh"
            exit 2
        ;;
    esac

    for dataset in ${datasets[@]}; do
        case $dataset in 
            beara) versions=58;;
            bearb-hour) versions=1299;;
            bearb-day) versions=89;;
            bearc) versions=32;;
            *)
                echo "Dataset must be in beara bearb-hour bearb-day bearc"
                exit 2
            ;;
        esac

        echo "Process is $policy, $dataset for JenaTDB2"
        total_ingestion_time=0
        total_file_size=0
        if [[ "$policy" == "tbsh" || "$policy" == "tbsf" || "$policy" == "tb" ]]; then
            repositoryID=${policy}_${dataset}
            # Load data into Jena
            ingestion_time=`(policy=${policy} dataset=${dataset} repositoryID=${repositoryID} rel_path_import_file=${datasetDirOrFile} \
                            time -p docker-compose run --rm jenatdb2_load) \
                            2>&1 1>> $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
            total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
            echo "\n\n" >> $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt
            file_size=`ls -l --block-size=k ~/.BEAR/rawdata/${dataset}/${datasetDirOrFile} | awk '{print substr($5, 1, length($5)-1)}'`
            total_file_size=`echo "$total_file_size + $file_size/1024" | bc`             

        elif [ "$policy" == "ic" ]; then
            for c in $(seq -f "%06g" 1 1) # ${versions}
            do
                repositoryID=${policy}_${dataset}_$((10#$c))

                # Load data into Jena
                ingestion_time=`(policy=${policy} dataset=${dataset} repositoryID=${repositoryID} rel_path_import_file=${datasetDirOrFile}/${c}.nt \
                                time -p docker-compose run --rm jenatdb2_load) \
                                2>&1 1>> $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt
                file_size=`ls -l --block-size=k ~/.BEAR/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  
            done
        
        elif [ "$policy" == "cb" ]; then
            for v in $(seq 0 1 2); do #versions -1
                ve=$(echo $v+1 | bc)
                if [ $v -eq 0 ]; then
                    fileadd="alldata.IC.nt/000001.nt"
                    filedel="empty.nt"
                    repositoryIDAdd=${policy}_${dataset}_ic1
                    repositoryIDDel=${policy}_${dataset}_empty
                else
                    fileadd="alldata.CB_computed.nt/data-added_$v-$ve.nt"
                    filedel="alldata.CB_computed.nt/data-deleted_$v-$ve.nt"
                    repositoryIDAdd=${policy}_${dataset}_add_$v-$ve
                    repositoryIDDel=${policy}_${dataset}_del_$v-$ve
                fi

                # Load data into Jena TDB2
                ingestion_time=`(policy=${policy} dataset=${dataset} repositoryID=${repositoryIDAdd} rel_path_import_file=${fileadd} \
                                time -p docker-compose run --rm jenatdb2_load) \
                                2>&1 1>> $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt
                file_size=`ls -l --block-size=k ~/.BEAR/rawdata/${dataset}/${fileadd} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  

                # Load data into Jena TDB2
                ingestion_time=`(policy=${policy} dataset=${dataset} repositoryID=${repositoryIDDel} rel_path_import_file=${filedel} \
                                time -p docker-compose run --rm jenatdb2_load) \
                                2>&1 1>> $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt 
                file_size=`ls -l --block-size=k ~/.BEAR/rawdata/${dataset}/${filedel} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`               
            done
        fi
        cat $baseDir/output/logs/${current_time}/jenaTDB2_logs.txt | grep -v "\[.*\] DEBUG"
        disk_usage=`du -s --block-size=M --apparent-size ~/.BEAR/databases/jenatdb2_${policy}_${dataset} | awk '{print substr($1, 1, length($1)-1)}'`
        echo "JenaTDB2;${policy};${dataset};${total_ingestion_time};${total_file_size};${disk_usage}" >> $baseDir/output/measurements/${current_time}/ingestion.txt 
    done
done

# TODO: log raw filesize and database filesize

# Remove dangling images
#docker rmi -f $(docker images -f "dangling=true" -q).

# DOCKER knowledge
# Dockerfile defines the build process for an image
# Parameters that are passed after the image (starvers_eval:latest) will be added after the ENTRYPOINT in the Dockerfile
# -it means interavtive + ttyp
# --rm will clean up the container ones docker shutsdown
# ARG variables are available during build time of the docker image
# ENV variables are available during runtime of the container
# Remove dangling images: docker rmi -f $(docker images -f "dangling=true" -q).