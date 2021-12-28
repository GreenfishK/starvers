#/bin/bash
data_dir=~/.BEAR
# rm -rf $data_dir/hdt-bearb-hour/cb/* # to clear database files created by hdt if the script needs to be re-executed

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

    time docker run \
        -it \
        --rm \
        -v $data_dir/hdt-bearb-hour/:/var/data/out/ \
        -v $data_dir/rawdata-bearb/hour/:/var/data/in/ \
        rfdhdt/hdt-cpp rdf2hdt -f ntriples $fileadd /var/data/out/cb/$v.add.hdt 
        > $data_dir/output/logs/load-bearb-hour-cb-$v-add-hdt.txt
    time docker run \
        -it \
        --rm \
        -v $data_dir/hdt-bearb-hour/:/var/data/out/ \
        -v $data_dir/rawdata-bearb/hour/:/var/data/in/ \
        rfdhdt/hdt-cpp rdf2hdt -f ntriples $filedel /var/data/out/cb/$v.del.hdt 
        > $data_dir/output/logs/load-bearb-hour-cb-$v-del-hdt.txt
done

