#/bin/bash
data_dir=~/.BEAR
# rm -rf $data_dir/tdb-bearb-hour/tb/* # to clear database files created by jena if the script needs to be re-executed

time docker run \
    -it \
    --rm \
    -v $data_dir/tdb-bearb-hour/:/var/data/out/ \
    -v $data_dir/rawdata-bearb/hour/:/var/data/in/ \
    stain/jena /jena/bin/tdbloader2 \
        --loc /var/data/out/tb /var/data/in/alldata.TB.nq \
    > $data_dir/output/logs/load-bearb-hour-tb--.txt

# stain/jena --sort-args "-S=16G" \ # returned an error message with the latest jena/stain image as of 04.12.2021
#docker run -it --rm \
#    -v $(pwd)/tdb/:/var/data/in/ \
#    -v $(pwd)/rawdata/alldata.IC.nq/:/var/data/out/ \
#    stain/jena /jena/bin/tdbloader2 --help
