[This](README_ORIG.md) was the original README.

# Reproducing experiments on jena

Execute all commands shown below in this directory of a working copy cloned from this repository.

## Prerequisites

### Docker

Have [docker](https://docs.docker.com/get-docker/) installed and [get access as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).

## Create docker image

```sh
docker build -t bear-jena .
```

## Put the data and queries in place

This step is needed to run locally. When running on server donizetti.labnet, the data is already there.

**TODO: Document how to create input, starting from [the BEAR documentation](https://aic.ai.wu.ac.at/qadlod/bear.html).**

```sh
# create directories
sudo mkdir /mnt/datastore
sudo chmod 777 /mnt/datastore/
mkdir -p /mnt/datastore/data/dslab/experimental/patch
# copy data; select:
# - beara, full
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/tdb /mnt/datastore/data/dslab/experimental/patch
# - bearb-day, full
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/tdb-bearb-day /mnt/datastore/data/dslab/experimental/patch
# - baerb-hour, full
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/tdb-bearb-hour /mnt/datastore/data/dslab/experimental/patch
# - beara, policy "cb" only
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/tdb/cb /mnt/datastore/data/dslab/experimental/patch/tdb
# - bearb-day, policy "cb" only
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/tdb-bearb-day/cb /mnt/datastore/data/dslab/experimental/patch/tdb-bearb-day
# - baerb-hour, policy "cb" only
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/tdb-bearb-hour/cb /mnt/datastore/data/dslab/experimental/patch/tdb-bearb-hour
# copy queries
rsync -rtvz donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_new /mnt/datastore/data/dslab/experimental/patch/BEAR
rsync -rtvz donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb /mnt/datastore/data/dslab/experimental/patch/BEAR
```

## Delete possible lock files from previous runs

```sh
sudo find /mnt/datastore/data/dslab/experimental/patch/tdb -iname '*.lock' -delete
sudo find /mnt/datastore/data/dslab/experimental/patch/tdb-bearb-day -iname '*.lock' -delete
sudo find /mnt/datastore/data/dslab/experimental/patch/tdb-bearb-hour -iname '*.lock' -delete
```

## Run the experiments

Note - on the server you may want to do this in a **screen** session.
```sh
./run-docker.sh beara && ./run-docker.sh bearb-day && ./run-docker.sh bearb-hour
```
