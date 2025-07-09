# Pull ostrich from DockerHub
`docker pull rdfostrich/ostrich`

# Pull HDT from docker and convert the initial snapshot to hdt
```
docker run --rm -v /mnt/data/starvers_eval/rawdata:/data rdfhdt/hdt-cpp rdf2hdt /data/beart/alldata.IC.nt/000001.nt /data/ostrich/000001.hdt

sudo cp /mnt/data/starvers_eval/rawdata/beart/alldata.CB_computed.nt/data-*.nt /mnt/data/starvers_eval/rawdata/ostrich/
```

# Insert the initial snapshot
```
docker run --rm -it --entrypoint /opt/ostrich/build/ostrich-insert -v /mnt/data/starvers_eval/rawdata/ostrich:/data rdfostrich/ostrich 1 + /data/alldata.CB_computed.nt/data-added_1-2.nt 
```

# Ingest initial snapshot
```
docker run --rm -it \
  -v /mnt/data/starvers_eval/rawdata/ostrich:/data \
  --workdir /data \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  0
```

# Add patches (initial snapshot or change sets)
```
docker run --rm -it \
  -v /mnt/data/starvers_eval/rawdata/ostrich:/data \
  --workdir /data \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  1 \
  + /data/000001.nt
```

# Query first snapshot
```
docker run --rm -it \
  -v /mnt/data/starvers_eval/rawdata/ostrich:/data \
  --workdir /data \
  --entrypoint /opt/ostrich/build/ostrich-query-version-materialized \
  rdfostrich/ostrich \
  1 s p o
```