This file replaces [the original README](README_ORIG.md) found in this directory.

# Reproducing experiments on HDT

## Prerequisites

### Docker

Have [docker](https://docs.docker.com/get-docker/) installed and [get access as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).

## Create docker image

Execute in the directory containing this README, in a working copy cloned from this repository.

```sh
docker build -t bear-hdt .
```

## Get input data and queries

See paragraph with the same name in [this common README](../common/README.md).

## Put the data and queries in place

This step is needed to run locally, assuming the data and queries at `donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/` are golden.

```sh
# create directories
sudo mkdir /mnt/datastore
sudo chmod 777 /mnt/datastore/
mkdir -p /mnt/datastore/data/dslab/experimental/patch
# copy data; select:
# - beara, full
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/beara-hdt /mnt/datastore/data/dslab/experimental/patch/
# - bearb-day, full
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt /mnt/datastore/data/dslab/experimental/patch
# - baerb-hour, full
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-hour-hdt /mnt/datastore/data/dslab/experimental/patch
# - beara, policy "cb" only
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/beara-hdt/cb /mnt/datastore/data/dslab/experimental/patch/beara-hdt
# - bearb-day, policy "cb" only
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt/cb /mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt
# - baerb-hour, policy "cb" only
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-hour-hdt/cb /mnt/datastore/data/dslab/experimental/patch/bearb-hour-hdt
# copy queries
rsync -rtvz donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_new /mnt/datastore/data/dslab/experimental/patch/BEAR
rsync -rtvz donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/BEAR/queries_bearb /mnt/datastore/data/dslab/experimental/patch/BEAR
```

## Delete possible index files from previous runs

```sh
sudo find /mnt/datastore/data/dslab/experimental/patch/beara-hdt -iname '*.index' -delete
sudo find /mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt -iname '*.index' -delete
sudo find /mnt/datastore/data/dslab/experimental/patch/bearb-hour-hdt -iname '*.index' -delete
```

## Run the experiments

Execute in the directory containing this README, in a working copy cloned from this repository.

Note - on the server you may want to do this in a **screen** session.

```sh
./run-docker.sh beara 2>&1 | tee beara.log
./run-docker.sh bearb-day 2>&1 | tee bearb-day.log
./run-docker.sh bearb-hour 2>&1 | tee bearb-hour.log
```
