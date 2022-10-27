#!/bin/bash

# Variables
baseDir=/starvers_eval
SCRIPT_DIR=/starvers_eval/scripts
policies=("${policies}") # cb tbsf tbsh tb
datasets=("${datasets}") # bearb_day beara bearc

echo "triple_store;policy;dataset;ingestion_time;raw_file_size_MiB;db_files_disk_usage_MiB" > $baseDir/output/measurements/ingestion.csv

### GraphDB ##################################################################
export JAVA_HOME=/opt/java/openjdk
export PATH=/opt/java/openjdk/bin:$PATH
graphdb_evns=$GDB_JAVA_OPTS
> $baseDir/output/logs/graphDB_logs.txt

for policy in ${policies[@]}; do
    case $policy in 
        ic) datasetDirOrFile=alldata.IC.nt;;
        cb) datasetDirOrFile=alldata.CB_computed.ttl;;
        tb) datasetDirOrFile=alldata.TB.nq;;
        tbsf) datasetDirOrFile=alldata.TB_star_flat.ttl;;
        tbsh) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
        *)
            echo "Policy must be in ic, cb, tb, tbsf, tbsh"
            exit 2
        ;;
    esac

    for dataset in ${datasets[@]}; do
        echo $dataset
        case $dataset in 
            beara) versions=58 file_name_struc="%01g";;
            bearb_hour) versions=1299 file_name_struc="%06g";; 
            bearb_day) versions=89 file_name_struc="%06g";;
            bearc) versions=32 file_name_struc="%01g";;
            *)
                echo "graphdb: Dataset must be in beara bearb_hour bearb_day bearc"
                exit 2
            ;;
        esac
        export GDB_JAVA_OPTS="$graphdb_evns -Dgraphdb.home.data=${baseDir}/databases/graphdb_${policy}_${dataset}/data"
        
        echo "Process is $policy, $dataset for GraphDB"
        total_ingestion_time=0
        total_file_size=0
        if [[ "$policy" == "tbsh" || "$policy" == "tbsf" || "$policy" == "tb" ]]; then
            # Replace repositoryID in config template
            repositoryID=${policy}_${dataset}
            cp ${SCRIPT_DIR}/2_load_data/configs/graphdb-config_template.ttl ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl
            sed -i "s/{{repositoryID}}/$repositoryID/g" ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl

            # Load data into GraphDB
            ingestion_time=`(time -p /opt/graphdb/dist/bin/preload -c ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl ${baseDir}/rawdata/${dataset}/${datasetDirOrFile} --force) \
                            2>&1 1>> $baseDir/output/logs/graphDB_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
            total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
            echo "\n\n" >> $baseDir/output/logs/graphDB_logs.txt
            file_size=`ls -l --block-size=k ${baseDir}/rawdata/${dataset}/${datasetDirOrFile} | awk '{print substr($5, 1, length($5)-1)}'`
            total_file_size=`echo "$total_file_size + $file_size/1024" | bc` 

        elif [ "$policy" == "ic" ]; then
            for c in $(seq -f $file_name_struc 1 ${versions}) # ${versions}
            do
                # Replace repositoryID in config template
                repositoryID=${policy}_${dataset}_$((10#$c))
                cp ${SCRIPT_DIR}/2_load_data/configs/graphdb-config_template.ttl ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl
                sed -i "s/{{repositoryID}}/$repositoryID/g" ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl

                # Load data into GraphDB
                ingestion_time=`(time -p /opt/graphdb/dist/bin/preload -c ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl ${baseDir}/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt --force) \
                                2>&1 1>> $baseDir/output/logs/graphDB_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/graphDB_logs.txt
                file_size=`ls -l --block-size=k ${baseDir}/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt  | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`
            done
        
        elif [ "$policy" == "cb" ]; then
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
                cp ${SCRIPT_DIR}/2_load_data/configs/graphdb-config_template.ttl ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl
                sed -i "s/{{repositoryID}}/$repositoryIDAdd/g" ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl

                # Load data into GraphDB
                ingestion_time=`(time -p /opt/graphdb/dist/bin/preload -c ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl ${baseDir}/rawdata/${dataset}/${fileadd} --force) \
                                2>&1 1>> $baseDir/output/logs/graphDB_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/graphDB_logs.txt
                file_size=`ls -l --block-size=k ${baseDir}/rawdata/${dataset}/${fileadd} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`

                # Delete
                # Replace repositoryID in config template
                cp ${SCRIPT_DIR}/2_load_data/configs/graphdb-config_template.ttl ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl
                sed -i "s/{{repositoryID}}/$repositoryIDDel/g" ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl

                # Load data into GraphDB
                ingestion_time=`(time -p /opt/graphdb/dist/bin/preload -c ${SCRIPT_DIR}/2_load_data/configs/graphdb-config.ttl ${baseDir}/rawdata/${dataset}/${filedel} --force) \
                                2>&1 1>> $baseDir/output/logs/graphDB_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/graphDB_logs.txt     
                file_size=`ls -l --block-size=k ${baseDir}/rawdata/${dataset}/${filedel} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`
            done
        fi
        cat $baseDir/output/logs/graphDB_logs.txt | grep -v "\[.*\] DEBUG"
        disk_usage=`du -s --block-size=M --apparent-size ${baseDir}/databases/graphdb_${policy}_${dataset}/data/repositories | awk '{print substr($1, 1, length($1)-1)}'`
        echo "GraphDB;${policy};${dataset};${total_ingestion_time};${total_file_size};${disk_usage}" >> $baseDir/output/measurements/ingestion.csv  
    done
done

### JenaTDB2 #################################################################
export JAVA_HOME=/usr/local/openjdk-11
export PATH=/usr/local/openjdk-11/bin:$PATH
export FUSEKI_HOME=/jena-fuseki
> $baseDir/output/logs/jenaTDB2_logs.txt

for policy in ${policies[@]}; do
    case $policy in 
        ic) datasetDirOrFile=alldata.IC.nt;;
        cb) datasetDirOrFile=alldata.CB_computed.ttl;;
        tb) datasetDirOrFile=alldata.TB.nq;;
        tbsf) datasetDirOrFile=alldata.TB_star_flat.ttl;;
        tbsh) datasetDirOrFile=alldata.TB_star_hierarchical.ttl;;
        *)
            echo "Policy must be in ic, cb, tb, tbsf, tbsh"
            exit 2
        ;;
    esac

    for dataset in ${datasets[@]}; do
        echo $dataset
        case $dataset in 
            beara) versions=58 file_name_struc="%01g";;
            bearb_hour) versions=1299 file_name_struc="%06g";; 
            bearb_day) versions=89 file_name_struc="%06g";;
            bearc) versions=32 file_name_struc="%01g";;
            *)
                echo "jenatdb2: Dataset must be in beara bearb_hour bearb_day bearc"
                exit 2
            ;;
        esac

        echo "Process is $policy, $dataset for JenaTDB2"
        total_ingestion_time=0
        total_file_size=0
        if [[ "$policy" == "tbsh" || "$policy" == "tbsf" || "$policy" == "tb" ]]; then
            repositoryID=${policy}_${dataset}
            # Replace repositoryID in config template
            mkdir ${baseDir}/configs/jenatdb2_${policy}_${dataset}
            cp ${SCRIPT_DIR}/2_load_data/configs/jenatdb2-config_template.ttl ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
            sed -i "s/{{repositoryID}}/$repositoryID/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
            sed -i "s/{{policy}}/$policy/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
            sed -i "s/{{dataset}}/$dataset/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl

            # Load data into Jena
            ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc ${baseDir}/databases/jenatdb2_${policy}_${dataset}/${repositoryID} ${baseDir}/rawdata/${dataset}/${datasetDirOrFile}) \
                            2>&1 1>> $baseDir/output/logs/jenaTDB2_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
            total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
            echo "\n\n" >> $baseDir/output/logs/jenaTDB2_logs.txt
            file_size=`ls -l --block-size=k ${baseDir}/rawdata/${dataset}/${datasetDirOrFile} | awk '{print substr($5, 1, length($5)-1)}'`
            total_file_size=`echo "$total_file_size + $file_size/1024" | bc`             

        elif [ "$policy" == "ic" ]; then
            mkdir ${baseDir}/configs/jenatdb2_${policy}_${dataset}
            for c in $(seq -f $file_name_struc 1 ${versions})
            do
                repositoryID=${policy}_${dataset}_$((10#$c))
                # Replace repositoryID in config template
                cp ${SCRIPT_DIR}/2_load_data/configs/jenatdb2-config_template.ttl ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{repositoryID}}/$repositoryID/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{policy}}/$policy/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{dataset}}/$dataset/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                
                # Load data into Jena
                ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc ${baseDir}/databases/jenatdb2_${policy}_${dataset}/${repositoryID} ${baseDir}/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt) \
                                2>&1 1>> $baseDir/output/logs/jenaTDB2_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/jenaTDB2_logs.txt
                file_size=`ls -l --block-size=k ${baseDir}/rawdata/${dataset}/${datasetDirOrFile}/${c}.nt | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  
            done
        
        elif [ "$policy" == "cb" ]; then
            mkdir ${baseDir}/configs/jenatdb2_${policy}_${dataset}
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
                cp ${SCRIPT_DIR}/2_load_data/configs/jenatdb2-config_template.ttl ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryIDAdd}.ttl
                sed -i "s/{{repositoryID}}/$repositoryIDAdd/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryIDAdd}.ttl
                sed -i "s/{{policy}}/$policy/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{dataset}}/$dataset/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl

                # Load data into Jena TDB2
                ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc ${baseDir}/databases/jenatdb2_${policy}_${dataset}/${repositoryIDAdd} ${baseDir}/rawdata/${dataset}/${fileadd}) \
                                2>&1 1>> $baseDir/output/logs/jenaTDB2_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/jenaTDB2_logs.txt
                file_size=`ls -l --block-size=k ${baseDir}/rawdata/${dataset}/${fileadd} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`  

                # Replace repositoryID in config template
                cp ${SCRIPT_DIR}/2_load_data/configs/jenatdb2-config_template.ttl ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryIDDel}.ttl
                sed -i "s/{{repositoryID}}/$repositoryIDDel/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryIDDel}.ttl
                sed -i "s/{{policy}}/$policy/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl
                sed -i "s/{{dataset}}/$dataset/g" ${baseDir}/configs/jenatdb2_${policy}_${dataset}/${repositoryID}.ttl

                # Load data into Jena TDB2
                ingestion_time=`(time -p /jena-fuseki/tdbloader2 --loc ${baseDir}/databases/jenatdb2_${policy}_${dataset}/${repositoryIDDel} ${baseDir}/rawdata/${dataset}/${filedel}) \
                                2>&1 1>> $baseDir/output/logs/jenaTDB2_logs.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/jenaTDB2_logs.txt 
                file_size=`ls -l --block-size=k ${baseDir}/rawdata/${dataset}/${filedel} | awk '{print substr($5, 1, length($5)-1)}'`
                total_file_size=`echo "$total_file_size + $file_size/1024" | bc`               
            done
        fi
        cat $baseDir/output/logs/jenaTDB2_logs.txt | grep -v "\[.*\] DEBUG"
        disk_usage=`du -s --block-size=M --apparent-size ${baseDir}/databases/jenatdb2_${policy}_${dataset} | awk '{print substr($1, 1, length($1)-1)}'`
        echo "JenaTDB2;${policy};${dataset};${total_ingestion_time};${total_file_size};${disk_usage}" >> $baseDir/output/measurements/ingestion.csv 
    done
done


# Remove dangling images
# docker rmi -f $(docker images -f "dangling=true" -q).

# DOCKER knowledge
# Dockerfile defines the build process for an image
# Parameters that are passed after the image (starvers_eval:latest) will be added after the ENTRYPOINT in the Dockerfile
# -it means interavtive + ttyp
# --rm will clean up the container ones docker shutsdown
# ARG variables are available during build time of the docker image
# ENV variables are available during runtime of the container
# Remove dangling images: docker rmi -f $(docker images -f "dangling=true" -q).