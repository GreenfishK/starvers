#!/bin/bash

# Variables
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
            docker run --name starvers_graphdb_${policy}_${dataset} \
                    -it \
                    --rm \
                    -v ~/.BEAR/databases/graphdb_${repositoryID}:/opt/graphdb/home \
                    -v ~/.BEAR/rawdata/${dataset}:/opt/graphdb/home/graphdb-import \
                    starvers_eval:latest \
                    /opt/graphdb/dist/bin/preload -c /opt/graphdb/dist/conf/${configFile} /opt/graphdb/home/graphdb-import/${datasetDirOrFile} --force

            # Run the graphdb database instance
            #docker run --name starvers_graphdb_${policy}_${dataset} \
            #        -it \
            #        --rm \
            #        -v ~/.BEAR/databases/graphdb_${repositoryID}:/opt/graphdb/home \
            #        -p 127.0.0.1:7200:7200 \
            #        starvers_eval:latest \
            #        /opt/graphdb/dist/bin/graphdb    
        elif [ "$policy" == "ic" ]; then
            echo "Policy is ic"
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