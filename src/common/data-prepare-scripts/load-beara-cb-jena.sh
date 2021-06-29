#/bin/bash

for v in $(seq 0 1 9); do
    echo $v
    ve=$(echo $v+1 | bc)
    if [ $v -eq 0 ]; then
        fileadd="/var/data/in/alldata.IC.nt/1.nt"
        filedel="/var/data/in/empty.nt"
    else
        fileadd="/var/data/in/alldata.CB.nt/data-added_$v-$ve.nt"
        filedel="/var/data/in/alldata.CB.nt/data-deleted_$v-$ve.nt"
    fi

    mkdir tdb/cb/$v
    time docker run -it --rm \
        -v $(pwd)/tdb/:/var/data/out/ \
        -v $(pwd)/rawdata/:/var/data/in/ \
        stain/jena /jena/bin/tdbloader2 --sort-args "-S=16G" --loc /var/data/out/cb/$v/add $fileadd > output/load-cb-$v-add-.txt
    time docker run -it --rm \
        -v $(pwd)/tdb/:/var/data/out/ \
        -v $(pwd)/rawdata/:/var/data/in/ \
        stain/jena /jena/bin/tdbloader2 --sort-args "-S=16G" --loc /var/data/out/cb/$v/del $filedel > output/load-cb-$v-del-.txt
done

#docker run -it --rm \
#    -v $(pwd)/tdb/:/var/data/in/ \
#    -v $(pwd)/rawdata/alldata.IC.nq/:/var/data/out/ \
#    stain/jena /jena/bin/tdbloader2 --help
