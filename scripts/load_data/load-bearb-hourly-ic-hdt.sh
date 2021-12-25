#/bin/bash
files=$(echo ~/.BEAR/rawdata-bearb/hour/alldata.IC.nt/*.nt | sed "s%~/.BEAR/rawdata-bearb/hour/alldata.IC.nt%/var/data/in%g")
sudo rm -rf ~/.BEAR/hdt-bearb-hour/ic/* # to clear database files created by hdt if the script needs to be re-executed

echo $files

for file in $files; do
    v=$(echo $file | sed "s/^.*\/\([0-9][0-9]*\)\.nt$/\1-1/" | bc)
    echo "$v"
    echo $file
    time docker run \
        -it \
        --rm \
        -v ~/.BEAR/hdt-bearb-hour/:/var/data/out/ \
        -v ~/.BEAR/rawdata-bearb/hour/alldata.IC.nt/:/var/data/in/ \
        rfdhdt/hdt-cpp rdf2hdt -f ntriples $file /var/data/out/ic/$v.hdt \
        > ~/.BEAR/output/load-bearb-hour-ic-$v-hdt.txt
done

