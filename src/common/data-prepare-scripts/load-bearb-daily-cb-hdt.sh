#/bin/bash

for v in $(seq 0 1 88); do
    echo $v
    ve=$(echo $v+1 | bc)
    if [ $v -eq 0 ]; then
        fileadd="/var/data/in/alldata.IC.nt/000001.nt"
        filedel="/var/data/in/empty.nt"
    else
        fileadd="/var/data/in/alldata.CB.nt/data-added_$v-$ve.nt"
        filedel="/var/data/in/alldata.CB.nt/data-deleted_$v-$ve.nt"
    fi

    time docker run -it --rm \
        -v $(pwd)/bearb-day-hdt/:/var/data/out/ \
        -v $(pwd)/rawdata-bearb/day/:/var/data/in/ \
        rfdhdt/hdt-cpp rdf2hdt -f ntriples $fileadd /var/data/out/cb/$v.add.hdt > output/load-bearb-day-cb-$v-add-hdt.txt
    time docker run -it --rm \
        -v $(pwd)/bearb-day-hdt/:/var/data/out/ \
        -v $(pwd)/rawdata-bearb/day/:/var/data/in/ \
        rfdhdt/hdt-cpp rdf2hdt -f ntriples $filedel /var/data/out/cb/$v.del.hdt > output/load-bearb-day-cb-$v-del-hdt.txt
done

