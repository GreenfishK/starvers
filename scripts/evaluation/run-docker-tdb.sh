#!/bin/bash

case "$1" in
  beara)
    datasetdir=~/.BEAR/databases/tdb-beara/
    querydir=~/.BEAR/queries/queries_new/
    outputdir=~/.BEAR/output/time/tdb/beara/
    limit=9
    ;;
  bearb-day)
    datasetdir=~/.BEAR/databases/tdb-bearb-day/
    querydir=~/.BEAR/queries/queries_bearb/
    outputdir=~/.BEAR/output/time/tdb/bearb-day/
    limit=88
    ;;
  bearb-hour)
    datasetdir=~/.BEAR/databases/tdb-bearb-hour/
    querydir=~/.BEAR/queries/queries_bearb/
    outputdir=~/.BEAR/output/time/tdb/bearb-hour/
    limit=1298
    ;;
  *)
    echo "Usage: $0 {beara|bearb-day|bearb-hour}"
    exit 2
    ;;
esac


policies="tb" # tb tb_star ic cb cbtb"
categories="mat" # mat diff ver
queries=$(cd ${querydir} && ls -v)
echo ${queries}

for policy in ${policies[@]}; do
for category in ${categories[@]}; do
for query in ${queries[@]}; do

echo "===== Running docker for ${policy}, ${category}, ${query} "
docker run \
    -it \
    --rm \
    -v ${datasetdir}:/var/data/dataset/ \
    -v ${querydir}:/var/data/queries/ \
    -v ${outputdir}:/var/data/output/ \
    bear-jena \
    java -cp target/tdbQuery-0.6-jar-with-dependencies.jar org/ai/wu/ac/at/tdbArchive/tools/JenaTDBArchive_query \
        -e ${limit} \
        -j 1 \
        -p ${policy} \
        -d /var/data/dataset/${policy} \
        -r spo \
        -c ${category} \
        -a /var/data/queries/${query} \
        -t /var/data/output/time-${policy}-${category}-$(echo ${query} | sed "s/\//-/g").csv
done
done
done

# Move to directory with local host name and local timestamp
lokal_timestamp="$(TZ=UTC-1 date "+%Y-%m-%dT%H:%M:%S")"
sudo mkdir ${outputdir}/${HOSTNAME}-${lokal_timestamp}
sudo mv ${outputdir}/time* ${outputdir}/${HOSTNAME}-${lokal_timestamp}
