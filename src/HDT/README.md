[This](README_ORIG.md) was the original README.

# Reproducing experiments on HDT

Execute all commands shown below in this directory of a working copy cloned from this repository.

## Prerequisites

Have [docker](https://docs.docker.com/get-docker/) installed. At time of writing, version `Docker version 20.10.6, build 370c289` was used on Ubuntu 18.04.

## Create docker

```sh
docker build -t bear-hdt .
```

## Put the BEAR data and queries in place

Script `run-docker.sh` assumes:

| item | available in local directory |
| ---- | ---------------------------- |
| data | `/mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt/` |
| queries | `/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb/` |
| output | `/mnt/datastore/data/dslab/experimental/patch/output/` |

For the time being, create directories and copy input from a well-known source:
```sh
sudo mkdir /mnt/datastore
sudo chmod 777 /mnt/datastore/
mkdir -p /mnt/datastore/data/dslab/experimental/patch
rsync -rtv donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt /mnt/datastore/data/dslab/experimental/patch
rsync -rtv donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb /mnt/datastore/data/dslab/experimental/patch/BEAR
```

**TODO: Document how to create input, starting from [the BEAR documentation](https://aic.ai.wu.ac.at/qadlod/bear.html).**

## Run the experiment

```sh
./run-docker.sh
```
