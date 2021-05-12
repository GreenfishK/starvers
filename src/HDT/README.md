[This](README_ORIG.md) was the original README.

# Reproducing experiments on HDT

Execute all commands shown below in this directory of a working copy cloned from this repository.

## Prerequisites

### Docker

Have [docker](https://docs.docker.com/get-docker/) installed and [get access as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).
If the latter is not possible, prefix docker commands and scripts calling docker commands with `sudo`.

## Create docker image

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

**TODO: Document how to create input, starting from [the BEAR documentation](https://aic.ai.wu.ac.at/qadlod/bear.html).**

Temporary workaround:
```sh
# create directories
sudo mkdir /mnt/datastore
sudo chmod 777 /mnt/datastore/
mkdir -p /mnt/datastore/data/dslab/experimental/patch
# copy data from where it was created earlier
rsync -rtv donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt /mnt/datastore/data/dslab/experimental/patch
rsync -rtv donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb /mnt/datastore/data/dslab/experimental/patch/BEAR
# fix the data for HDT
sudo find /mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt -iname '*.index' -delete
```


## Run the experiment

```sh
./run-docker.sh
```
