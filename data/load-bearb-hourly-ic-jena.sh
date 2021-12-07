#/bin/bash
files=$(echo $(pwd)/rawdata-bearb/hour/alldata.IC.nt/*.nt | sed "s%$(pwd)/rawdata-bearb/hour/alldata.IC.nt%/var/data/in%g")
sudo rm -rf $(pwd)/tdb-bearb-hour/ic/* # to clear database files created by jena if the script needs to be re-executed

for file in $files; do
    v=$(echo $file | sed "s/^.*\/\([0-9][0-9]*\)\.nt$/\1-1/" | bc)
    #if [ $v -lt 10 ]; then
        echo "$v"
        time docker run \
            -it \
            --rm \
            -v $(pwd)/tdb-bearb-hour/:/var/data/out/ \
            -v $(pwd)/rawdata-bearb/hour/alldata.IC.nt/:/var/data/in/ \
            stain/jena /jena/bin/tdbloader2 \
                --loc /var/data/out/ic/$v $file \
            > output/load-bearb-hour-ic-$v-.txt
    #fi
done

# line 14: --sort-args "-S=16G" \ # returned an error message with the latest jena/stain image as of 04.12.2021 --> removed sort-args

#docker run -it --rm \
#    -v $(pwd)/tdb/:/var/data/in/ \
#    -v $(pwd)/rawdata/alldata.IC.nq/:/var/data/out/ \
#    stain/jena /jena/bin/tdbloader2 --help

# docker volumes https://docs.docker.com/storage/volumes/
# host volumes: -v <path/on/host>:<path/in/container>
# named volumes: -v <name>:<path/in/container>
