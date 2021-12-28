#/bin/bash
data_dir=~/.BEAR
# rm -rf $data_dir/tdb-bearb-hour/cb/* # to clear database files created by jena if the script needs to be re-executed

for v in $(seq 0 1 1298); do
    echo $v
    ve=$(echo $v+1 | bc)
    if [ $v -eq 0 ]; then
        fileadd="/var/data/in/alldata.IC.nt/000001.nt"
        filedel="/var/data/in/empty.nt"
    else
        fileadd="/var/data/in/alldata.CB.nt/data-added_$v-$ve.nt"
        filedel="/var/data/in/alldata.CB.nt/data-deleted_$v-$ve.nt"
    fi

    mkdir $data_dir/tdb-bearb-hour/cb/$v
    time docker run \
        -it \
        --rm \
        -v $data_dir/tdb-bearb-hour/:/var/data/out/ \
        -v $data_dir/rawdata-bearb/hour/:/var/data/in/ \
        stain/jena /jena/bin/tdbloader2 \
            --loc /var/data/out/cb/$v/add $fileadd \
        > $data_dir/output/logs/load-bearb-hour-cb-$v-add-.txt
    time docker run \
        -it \
        --rm \
        -v $data_dir/tdb-bearb-hour/:/var/data/out/ \
        -v $data_dir/rawdata-bearb/hour/:/var/data/in/ \
        stain/jena /jena/bin/tdbloader2 \
            --loc /var/data/out/cb/$v/del $filedel \
        > $data_dir/output/logs/load-bearb-hour-cb-$v-del-.txt
done

# stain/jena --sort-args "-S=16G" \ # returned an error message with the latest jena/stain image as of 04.12.2021

#docker run -it --rm \
#    -v $(pwd)/tdb/:/var/data/in/ \
#    -v $(pwd)/rawdata/alldata.IC.nq/:/var/data/out/ \
#    stain/jena /jena/bin/tdbloader2 --help
