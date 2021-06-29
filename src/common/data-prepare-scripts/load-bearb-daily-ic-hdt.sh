#/bin/bash
files=$(echo $(pwd)/rawdata-bearb/day/alldata.IC.nt/*.nt | sed "s%$(pwd)/rawdata-bearb/day/alldata.IC.nt%/var/data/in%g")
echo $files

for file in $files; do
    v=$(echo $file | sed "s/^.*\/\([0-9][0-9]*\)\.nt$/\1-1/" | bc)
    echo "$v"
    echo $file
    time docker run -it --rm \
        -v $(pwd)/bearb-day-hdt/:/var/data/out/ \
        -v $(pwd)/rawdata-bearb/day/alldata.IC.nt/:/var/data/in/ \
        rfdhdt/hdt-cpp rdf2hdt -f ntriples $file /var/data/out/ic/$v.hdt > output/load-bearb-day-ic-$v-hdt.txt
done

