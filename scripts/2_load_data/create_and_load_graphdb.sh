#!/bin/bash

# Variables
baseDir=~/.BEAR
configFile=graphdb-config.ttl
policies="ic" # cb tbsf tbsh tb
datasets="bearb-hour" # bearb-day beara bearc

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

        # Replace repositoryID in config template
        repositoryID=${policy}_${dataset}
        cp configs/graphdb-config_template.ttl configs/graphdb-config.ttl
        sed -i "s/{{repositoryID}}/$repositoryID/g" configs/graphdb-config.ttl

        # Build GraphDB image and copy config file and license
        docker build --build-arg configFile=${configFile} -t starvers_eval . 

        if [[ "$policy" == "tbsh" || "$policy" == "tbsf" || "$policy" == "tb" ]]; then
            # Load data
            time docker run --name starvers_graphdb_${policy}_${dataset} \
                            -it \
                            --rm \
                            -v ~/.BEAR/databases/graphdb_${repositoryID}:/opt/graphdb/home \
                            -v ~/.BEAR/rawdata/${dataset}:/opt/graphdb/home/graphdb-import \
                            starvers_eval:latest \
                            /opt/graphdb/dist/bin/preload -c /opt/graphdb/dist/conf/${configFile} /opt/graphdb/home/graphdb-import/${datasetDirOrFile} --force \
                            > $baseDir/output/logs/${policy}/${dataset}/ingestion.txt

        elif [ "$policy" == "ic" ]; then
            echo "Policy is ic"
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
            for c in $(seq -f "%06g" 1 ${versions}) 
            do
                # Replace repositoryID in config template
                repositoryID=${policy}_${dataset}_$((10#$c))
                cp configs/graphdb-config_template.ttl configs/graphdb-config.ttl
                sed -i "s/{{repositoryID}}/$repositoryID/g" configs/graphdb-config.ttl

                # Build GraphDB image and copy config file and license
                docker build --build-arg configFile=${configFile} -t starvers_eval . 

                ingestion_time=`(time -p docker run --name starvers_graphdb_${policy}_${dataset} \
                                -it \
                                --rm \
                                -v ~/.BEAR/databases/graphdb_${policy}_${dataset}:/opt/graphdb/home \
                                -v ~/.BEAR/rawdata/${dataset}:/opt/graphdb/home/graphdb-import \
                                starvers_eval:latest \
                                /opt/graphdb/dist/bin/preload -c /opt/graphdb/dist/conf/${configFile} /opt/graphdb/home/graphdb-import/${datasetDirOrFile}/${c}.nt --force) \
                                2>&1 | grep -oP "real \K.*" | sed "s/,/./g" `
                total_ingestion_time=`echo "$total_ingestion_time + $ingestion_time" | bc`
            done
            echo "GraphDB;${policy};${dataset};${total_ingestion_time}" >> $baseDir/output/logs/ingestion.txt 

        elif [ "$policy" == "cb" ]; then
            echo "Policy is cb"
        fi
    done
done


# Remove dangling images
docker rmi -f $(docker images -f "dangling=true" -q).

# DOCKER knowledge
# Parameters that are passed after the image (starvers_eval:latest) will be added after the ENTRYPOINT in the Dockerfile
# -it means interavtive + ttyp
# --rm will clean up the container ones docker shutsdown
# ARG variables are available during build time of the docker image
# ENV variables are available during runtime of the container
# Remove dangling images: docker rmi -f $(docker images -f "dangling=true" -q).