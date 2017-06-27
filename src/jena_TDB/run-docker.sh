#!/bin/bash

docker run -it --rm \
    -e POLICY="ic" \
    -e ROLE="s" \
    -e CATEGORY="mat" \
    -e QUERY="subjectLookup/queries-sel-10-e0.2.txt" \
    -v /mnt/datastore/data/dslab/experimental/patch/tdb/:/var/data/dataset/ \
    -v /mnt/datastore/data/dslab/experimental/patch/BEAR/queries/:/var/data/queries/ \
    -v /mnt/datastore/data/dslab/experimental/patch/output/:/var/data/output/ \
    bear-jena
