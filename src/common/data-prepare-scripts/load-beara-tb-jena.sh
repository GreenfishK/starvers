#/bin/bash
time docker run -it --rm \
    -v $(pwd)/tdb/:/var/data/out/ \
    -v $(pwd)/rawdata/:/var/data/in/ \
    stain/jena /jena/bin/tdbloader2 --sort-args "-S=16G" --loc /var/data/out/tb /var/data/in/alldata.TB.nq > output/load-tb--.txt

#docker run -it --rm \
#    -v $(pwd)/tdb/:/var/data/in/ \
#    -v $(pwd)/rawdata/alldata.IC.nq/:/var/data/out/ \
#    stain/jena /jena/bin/tdbloader2 --help
