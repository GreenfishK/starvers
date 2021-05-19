#!/bin/bash

case "$1" in
  beara)
    datasetdir=/mnt/datastore/data/dslab/experimental/patch/tdb/
    querydir=/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_new/
    outputdir=/mnt/datastore/data/dslab/experimental/patch/output/time/beara/
    limit=9
    ;;
  bearb-day)
    datasetdir=/mnt/datastore/data/dslab/experimental/patch/tdb-bearb-day/
    querydir=/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb/
    outputdir=/mnt/datastore/data/dslab/experimental/patch/output/time/bearb-day/
    limit=88
    ;;
  bearb-hour)
    datasetdir=/mnt/datastore/data/dslab/experimental/patch/tdb-bearb-hour/
    querydir=/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb/
    outputdir=/mnt/datastore/data/dslab/experimental/patch/output/time/bearb-hour/
    limit=1298
    ;;
  *)
    echo "Usage: $0 {beara|bearb-day|bearb-hour}"
    exit 2
    ;;
esac

policies="ic cb tb cbtb"
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

echo "===== Running docker for ${policy}, ${category}, ${query} "
docker run -it --rm \
    -v ${datasetdir}:/var/data/dataset/ \
    -v ${querydir}:/var/data/queries/ \
    -v ${outputdir}:/var/data/output/ \
    bear-jena \
    java -cp target/tdbQuery-0.6-jar-with-dependencies.jar org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query \
    -e ${limit} \
    -j 1 \
    -p ${policy} \
    -d /var/data/dataset/${policy} \
    -r spo \
    -c ${category} \
    -a /var/data/queries/${query} \
    -t /var/data/output/time-${policy}-${category}-$(echo ${query} | sed "s/\//-/g").txt
done
done
done
