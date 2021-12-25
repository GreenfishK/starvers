bin/bash
sudo rm -rf ~/.BEAR/tdb-bearb-hour/tb_star/* # to clear database files created by jena if the script needs to be re-executed

time docker run \
    -it \
    --rm \
    -v ~/.BEAR/tdb-bearb-hour/:/var/data/out/ \
    -v ~/.BEAR/rawdata-bearb/hour/:/var/data/in/ \
    stain/jena /jena/bin/tdbloader2 \
        --loc /var/data/out/tb_star /var/data/in/alldata.TB_star.ttl \
    > ~/.BEAR/output/load-bearb-hour-tb_star--.txt

# stain/jena --sort-args "-S=16G" \ # returned an error message with the latest jena/stain image as of 04.12.2021
#docker run -it --rm \
#    -v $(pwd)/tdb/:/var/data/in/ \
#    -v $(pwd)/rawdata/alldata.IC.nq/:/var/data/out/ \
#    stain/jena /jena/bin/tdbloader2 --help
