#!/bin/bash

case "$1" in
  beara)
    datasetdir=~/.BEAR/databases/hdt-beara/
    querydir=~/.BEAR/queries/queries_new/
    outputdir=~/.BEAR/output/time/hdt/beara/
    limit=10
    ;;
  bearb-day)
    datasetdir=~/.BEAR/databases/hdt-bearb-day/
    querydir=~/.BEAR/queries/queries_bearb/
    outputdir=~/.BEAR/output/time/hdt/bearb-day/
    limit=88
    ;;
  bearb-hour)
    datasetdir=~/.BEAR/databases/hdt-bearb-hour/
    querydir=~/.BEAR/queries/queries_bearb/
    outputdir=~/.BEAR/output/time/hdt/bearb-hour/
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
#    queries="po-queries-lowCardinality.txt"
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
    bear-hdt \
    ./query-${policy}-${category} \
    -d /var/data/dataset/${policy}/ \
    -l ${limit} \
    -t spo \
    -i /var/data/queries/${query} \
    -o /var/data/output/time-${policy}-${category}-$(echo ${query} | sed "s/\//-/g").txt
done
done
done

# Move to directory with local host name and local timestamp
lokal_timestamp="$(TZ=UTC-1 date "+%Y-%m-%dT%H:%M:%S")"
sudo mkdir ${outputdir}/${HOSTNAME}-${lokal_timestamp}
sudo mv ${outputdir}/time* ${outputdir}/${HOSTNAME}-${lokal_timestamp}
