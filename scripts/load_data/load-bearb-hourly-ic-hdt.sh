#/bin/bash
data_dir=~/.BEAR
files=$(echo $data_dir/rawdata-bearb/hour/alldata.IC.nt/*.nt | sed "s%$data_dir/rawdata-bearb/hour/alldata.IC.nt%/var/data/in%g")
# rm -rf $data_dir/hdt-bearb-hour/ic/* # to clear database files created by hdt if the script needs to be re-executed

for file in $files; do
    v=$(echo $file | sed "s/^.*\/\([0-9][0-9]*\)\.nt$/\1-1/" | bc)
    echo "$v"
    echo $file
    time docker run \
        -it \
        --rm \
        -v $data_dir/hdt-bearb-hour/:/var/data/out/ \
        -v $data_dir/rawdata-bearb/hour/alldata.IC.nt/:/var/data/in/ \
        rfdhdt/hdt-cpp rdf2hdt -f ntriples $file /var/data/out/ic/$v.hdt \
        > $data_dir/output/load-bearb-hour-ic-$v-hdt.txt
done

