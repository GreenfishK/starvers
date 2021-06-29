#/bin/bash
time docker run -it --rm \
    -v $(pwd)/tdb-bearb-day/:/var/data/out/ \
    -v $(pwd)/rawdata-bearb/day/:/var/data/in/ \
    stain/jena /jena/bin/tdbloader2 --sort-args "-S=16G" --loc /var/data/out/cbtb /var/data/in/alldata.CBTB.nq > output/load-bearb-day-cbtb--.txt

#docker run -it --rm \
#    -v $(pwd)/tdb/:/var/data/in/ \
#    -v $(pwd)/rawdata/alldata.IC.nq/:/var/data/out/ \
#    stain/jena /jena/bin/tdbloader2 --help
