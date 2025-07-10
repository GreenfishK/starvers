# Pull ostrich from DockerHub
`docker pull rdfostrich/ostrich`

# Delete all database files
sudo rm -rf *_del* && sudo rm -rf *_add* && sudo rm -rf *.hdt && sudo rm -rf *.v1-1 && sudo rm -rf *.dic && sudo rm -rf *.dat && sudo rm -rf *.kch

# Ingest initial snapshot
```
docker run --rm -it \
  -v /mnt/data/starvers_eval:/data \
  --workdir /data/databases/ostrich \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  0
```

# Ingest the initial snapshot
```
docker run --rm -it \
  -v /mnt/data/starvers_eval:/data \
  --workdir /data/databases/ostrich \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  1 \
  + /data/rawdata/beart/alldata.IC.nt/000001.nt
```

# Insert patches (change sets)
```
docker run --rm -it \
  -v /mnt/data/starvers_eval:/data \
  --workdir /data/databases/ostrich \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  2 \
  + /data/rawdata/beart/alldata.CB_computed.nt/data-added_1-2.nt
```

```
docker run --rm -it \
  -v /mnt/data/starvers_eval:/data \
  --workdir /data/databases/ostrich \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  2 \
  + /data/rawdata/beart/alldata.CB_computed.nt/data-deleted_1-2.nt
```


# Query initial snapshot
```
docker run --rm -it \
  -v /mnt/data/starvers_eval/databases/ostrich:/data \
  --workdir /data \
  --entrypoint /opt/ostrich/build/ostrich-query-version-materialized \
  rdfostrich/ostrich \
  0 s p o
```