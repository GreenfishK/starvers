#!/bin/bash

# Variables
baseDir=~/.BEAR
policies="cb" # cb tbsf tbsh tb
datasets="bearb-hour" # bearb-day beara bearc
current_time=`date "+%Y-%m-%dT%H:%M:%S"`

# Build Jena image
docker build -f jenatdb2.Dockerfile -t starvers_eval . 

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

        total_ingestion_time=0
        if [[ "$policy" == "tbsh" || "$policy" == "tbsf" || "$policy" == "tb" ]]; then
            repositoryID=${policy}_${dataset}

            # Load data into Jena
            ingestion_time=`(time -p docker run \
                            --name starvers_jenatdb2_${policy}_${dataset} \
                            -it \
                            --rm \
                            -v ~/.BEAR/databases/jenatdb2_${repositoryID}:/var/data/out/ \
                            -v ~/.BEAR/rawdata/${dataset}:/var/data/in/ \
                            stain/jena \
                            /jena/bin/tdbloader2 --loc /var/data/out/${repositoryID} /var/data/in/${datasetDirOrFile}) \
                            2>&1 1>> $baseDir/output/logs/jenaTDB2_logs_${current_time}.txt | grep -oP "real \K.*" | sed "s/,/./g" `
            total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
            echo "\n\n" >> $baseDir/output/logs/jenaTDB2_logs_${current_time}.txt

        elif [ "$policy" == "ic" ]; then
            echo "Policy is ic"

            for c in $(seq -f "%06g" 1 1) # ${versions}
            do
                repositoryID=${policy}_${dataset}_$((10#$c))

                # Load data into Jena
                ingestion_time=`(time -p docker run \
                                --name starvers_jenatdb2_${policy}_${dataset} \
                                -it \
                                --rm \
                                -v ~/.BEAR/databases/jenatdb2_${policy}_${dataset}:/var/data/out/ \
                                -v ~/.BEAR/rawdata/${dataset}:/var/data/in/ \
                                stain/jena \
                                /jena/bin/tdbloader2 --loc /var/data/out/${repositoryID} /var/data/in/${datasetDirOrFile}/${c}.nt) \
                                2>&1 1>> $baseDir/output/logs/jenaTDB2_logs_${current_time}.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/jenaTDB2_logs_${current_time}.txt

                # Load data into Jena TDB
            done
        
        elif [ "$policy" == "cb" ]; then
            echo "Policy is cb"
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

                # Jena #########################################################################

                # Load data into Jena TDB2
                ingestion_time=`(time docker run \
                                --name starvers_jenatdb2_${policy}_${dataset} \
                                -it \
                                --rm \
                                -v ~/.BEAR/databases/jenatdb2_${policy}_${dataset}:/var/data/out/ \
                                -v ~/.BEAR/rawdata/${dataset}:/var/data/in/ \
                                stain/jena \
                                /jena/bin/tdbloader2 --loc /var/data/out/${repositoryIDAdd} /var/data/in/${fileadd}) \
                                2>&1 1>> $baseDir/output/logs/jenaTDB2_logs_${current_time}.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/jenaTDB2_logs_${current_time}.txt

                # Load data into Jena TDB2
                ingestion_time=`(time docker run \
                                --name starvers_jenatdb2_${policy}_${dataset} \
                                -it \
                                --rm \
                                -v ~/.BEAR/databases/jenatdb2_${policy}_${dataset}:/var/data/out/ \
                                -v ~/.BEAR/rawdata/${dataset}:/var/data/in/ \
                                stain/jena \
                                /jena/bin/tdbloader2 --loc /var/data/out/${repositoryIDDel} /var/data/in/${filedel}) \
                                2>&1 1>> $baseDir/output/logs/jenaTDB2_logs_${current_time}.txt | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
                echo "\n\n" >> $baseDir/output/logs/jenaTDB2_logs_${current_time}.txt                
            done
        fi
        echo "JenaTDB2;${policy};${dataset};${total_ingestion_time}" >> $baseDir/output/logs/ingestion_${current_time}.txt 
    done
done

# TODO: log raw filesize and database filesize
# TODO: Delete non-empty repositories to be able to rerun jena
# TODO: fix bug which leads to not logging the ingestion time

# Remove dangling images
docker rmi -f $(docker images -f "dangling=true" -q).

# DOCKER knowledge
# Parameters that are passed after the image (starvers_eval:latest) will be added after the ENTRYPOINT in the Dockerfile
# -it means interavtive + ttyp
# --rm will clean up the container ones docker shutsdown
# ARG variables are available during build time of the docker image
# ENV variables are available during runtime of the container
# Remove dangling images: docker rmi -f $(docker images -f "dangling=true" -q).