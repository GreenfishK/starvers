[This](README_ORIG.md) was the original README.

# Reproducing experiments on HDT

Execute all commands shown below in this directory of a working copy cloned from this repository.

## Prerequisites

### Docker

Have [docker](https://docs.docker.com/get-docker/) installed and [get access as non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).

## Create docker image

```sh
docker build -t bear-hdt .
```

## Put the data and queries in place

This step is needed to run locally. When running on server donizetti.labnet, the data is already there.

**TODO: Document how to create input, starting from [the BEAR documentation](https://aic.ai.wu.ac.at/qadlod/bear.html).**

See the script `run-docker.sh` to find out which data and queries are used for which experiment.

```sh
# create directories
sudo mkdir /mnt/datastore
sudo chmod 777 /mnt/datastore/
mkdir -p /mnt/datastore/data/dslab/experimental/patch
# copy data (full - lots of data!)
#rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/beara-hdt /mnt/datastore/data/dslab/experimental/patch/
#rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt /mnt/datastore/data/dslab/experimental/patch
#rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-hour-hdt /mnt/datastore/data/dslab/experimental/patch
# copy data (subset - sufficient for policy "cb" only)
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/beara-hdt/cb /mnt/datastore/data/dslab/experimental/patch/beara-hdt
rsync -rtvz --exclude='*.tar.gz' donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt/cb /mnt/datastore/data/dslab/experimental/patch/bearb-day-hdt
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

Note - on the server you may want to do this in a **screen** session.
```sh
./run-docker.sh beara && ./run-docker.sh bearb-day && ./run-docker.sh bearb-hour
```
