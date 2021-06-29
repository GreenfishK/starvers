#/bin/bash
files=$(echo $(pwd)/rawdata/alldata.IC.nt/*.nt | sed "s%$(pwd)/rawdata/alldata.IC.nt%/var/data/in%g")

for file in $files; do
    v=$(echo $file | sed "s/^.*\/\([0-9][0-9]*\)\.nt$/\1-1/" | bc)
    if [ $v -lt 10 ]; then
        echo "$v"
        time docker run -it --rm \
            -v $(pwd)/beara-hdt/:/var/data/out/ \
            -v $(pwd)/rawdata/alldata.IC.nt/:/var/data/in/ \
            rfdhdt/hdt-cpp rdf2hdt -f ntriples $file /var/data/out/ic/$v.hdt > output/load-beara-ic-$v-hdt.txt
    fi
done

