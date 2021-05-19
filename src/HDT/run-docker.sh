#!/bin/bash

case "$1" in
  beara)
    datasetdir=/mnt/datastore/data/dslab/experimental/patch/beara-hdt/
    querydir=/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_new/
    outputdir=/mnt/datastore/data/dslab/experimental/patch/output/time/beara/
    limit=10
    ;;
  bearb-day)
    datasetdir=/mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt/
    querydir=/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb/
    outputdir=/mnt/datastore/data/dslab/experimental/patch/output/time/bearb-day/
    limit=88
    ;;
  bearb-hour)
    datasetdir=/mnt/datastore/data/dslab/experimental/patch/bearb-hour-hdt/
    querydir=/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb/
    outputdir=/mnt/datastore/data/dslab/experimental/patch/output/time/bearb-hour/
    limit=1298
    ;;
  *)
    echo "Usage: $0 {beara|bearb-day|bearb-hour}"
    exit 2
    ;;
esac

policies="ic cb"
categories="mat diff ver"
queries=$(cd ${querydir} && ls -v)
echo ${queries}

# Overrides for local testing - to be put in comments in committed version
#policies="cb"
#categories="mat"
#case "$1" in
#  beara)
#    queries="o-queries-lowCardinality.txt"
#    ;;
#  bearb-day | bearb-hour)
#    queries="p.txt"
#    ;;
#esac
# End overrides for local testing

for policy in ${policies[@]}; do
for category in ${categories[@]}; do
for query in ${queries[@]}; do

docker run -it --rm \
    -v ${datasetdir}:/var/data/dataset/ \
    -v ${querydir}:/var/data/queries/ \
    -v ${outputdir}:/var/data/output/ \
    bear-hdt \
    ./query-${policy}-${category} \
    -d /var/data/dataset/${policy}/ \
    -l ${limit} \
    -t spo \
    -i /var/data/queries/${query} \
    -o /var/data/output/time-hdt-${policy}-${category}-$(echo ${query} | sed "s/\//-/g").txt
done
done
done
