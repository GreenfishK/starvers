policy=tb
dataset=bearb-hour
repositoryID=${policy}_${dataset}

docker run --name starvers_graphdb_${policy}_${dataset} \
        -it \
        --rm \
        -v ~/.BEAR/databases/graphdb_${repositoryID}:/opt/graphdb/home \
        -p 127.0.0.1:7200:7200 \
        starvers_eval:latest \
        /opt/graphdb/dist/bin/graphdb 