#/bin/bash
files=$(echo $(pwd)/rawdata-bearb/day/alldata.IC.nt/*.nt | sed "s%$(pwd)/rawdata-bearb/day/alldata.IC.nt%/var/data/in%g")

for file in $files; do
    v=$(echo $file | sed "s/^.*\/\([0-9][0-9]*\)\.nt$/\1-1/" | bc)
    #if [ $v -lt 10 ]; then
        echo "$v"
        time docker run -it --rm \
            -v $(pwd)/tdb-bearb-day/:/var/data/out/ \
            -v $(pwd)/rawdata-bearb/day/alldata.IC.nt/:/var/data/in/ \
            stain/jena /jena/bin/tdbloader2 --sort-args "-S=16G" --loc /var/data/out/ic/$v $file > output/load-bearb-day-ic-$v-.txt
    #fi
done

#docker run -it --rm \
#    -v $(pwd)/tdb/:/var/data/in/ \
#    -v $(pwd)/rawdata/alldata.IC.nq/:/var/data/out/ \
#    stain/jena /jena/bin/tdbloader2 --help
