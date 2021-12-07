#/bin/bash
files=$(echo $(pwd)/rawdata-bearb/hour/alldata.IC.nt/*.nt | sed "s%$(pwd)/rawdata-bearb/hour/alldata.IC.nt%/var/data/in%g")
sudo rm -rf $(pwd)/hdt-bearb-hour/ic/* # to clear database files created by hdt if the script needs to be re-executed

echo $files

for file in $files; do
    v=$(echo $file | sed "s/^.*\/\([0-9][0-9]*\)\.nt$/\1-1/" | bc)
    echo "$v"
    echo $file
    time docker run \
        -it \
        --rm \
        -v $(pwd)/hdt-bearb-hour/:/var/data/out/ \
        -v $(pwd)/rawdata-bearb/hour/alldata.IC.nt/:/var/data/in/ \
        rfdhdt/hdt-cpp rdf2hdt -f ntriples $file /var/data/out/ic/$v.hdt \
        > output/load-bearb-hour-ic-$v-hdt.txt
done

