# Pull ostrich from DockerHub
`docker pull rdfostrich/ostrich`

# Run ostrich endpoint
From the root project directory of starvers_eval run:
`docker-compose up -d start_ostrich_endpoint`

# Debug
```
docker run --rm -it \
  --entrypoint /bin/bash \
  rdfostrich/ostrich
```

# Test
```
docker run --rm -it \
  --entrypoint /opt/ostrich/build/ostrich_test \
  rdfostrich/ostrich
```

# Ingest the initial snapshot
```
docker run --rm -it \
  -v /mnt/data/starvers_eval:/data \
  --workdir /data/databases/ostrich \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  -v \
  0 \
  + /data/rawdata/beart_ostrich/alldata.IC.nt/000001.nt
```

# Insert patch (positive change set)
```
docker run --rm -it \
  -v /mnt/data/starvers_eval:/data \
  --workdir /data/databases/ostrich \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  -v \
  1 \
  + /data/rawdata/beart_ostrich/alldata.CB_computed.nt/data-added_1-2.nt
```

# Insert patch (negative change set)
```
docker run --rm -it \
  -v /mnt/data/starvers_eval:/data \
  --workdir /data/databases/ostrich \
  --entrypoint /opt/ostrich/build/ostrich-insert \
  rdfostrich/ostrich \
  -v \
  1 \
  - /data/rawdata/beart_ostrich/alldata.CB_computed.nt/data-deleted_1-2.nt
```
Adding a negative change set to version 1 gets stuck and never finishes.

# Query Ostrich Store
```
docker run --rm -it \
  -v /mnt/data/starvers_eval:/data \
  --workdir /data/databases/ostrich \
  --entrypoint /opt/ostrich/build/ostrich-query-version-materialized \
  rdfostrich/ostrich \
  -v \
  0 \
  s p o
```