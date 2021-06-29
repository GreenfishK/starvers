# Common information for HDT and jana_TDB experiments

## Get input data and queries

The input data and queries origin is in [the BEAR documentation](https://aic.ai.wu.ac.at/qadlod/bear.html).
Some of this data needs to be downloaded and optionally prepared for this experiment, as described below.

Here we try to reconstruct this process from what's found on `donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/`, without re-executing the steps,
which is not feasible in the current restrictions of time and disk space.
Script files are recovered from that server location as well and saved here in the subdirectory `data-prepare-scripts`.

You may skip this section if you assume the data at `donizetti.labnet:/mnt/datastore/data/dslab/experimental/patch/` is golden.

In what follows we assume there is a directory `/mnt/datastore/data/dslab/experimental/patch/` on the machine you're getting the input data and queries.

### Step 1: download and expand data

Note: for HDT, only the policies IC and CB are required. For jena, all policies listed are required.

| Location in [the BEAR documentation](https://aic.ai.wu.ac.at/qadlod/bear.html) | destination (after tar xvzf) (*) |
| ------------------------------------------------------------------------------ | -------------------------------- |
| 2. BEAR-A / Get the dataset / Policy IC | ./rawdata/alldata.IC.nt (directory) |
| 2. BEAR-A / Get the dataset / Policy CB | ./rawdata/alldata.CB.nt (directory) |
| 2. BEAR-A / Get the dataset / Policy TB | ./rawdata/alldata.TB.nq (file) |
| 2. BEAR-A / Get the dataset / Policy CBTB | ./rawdata/alldata.CBTB.nq (file) |
| 3. BEAR-B / Get the dataset / Granularity hour / Policy IC | ./rawdata-bearb/hour/alldata.IC.nt (directory) |
| 3. BEAR-B / Get the dataset / Granularity hour / Policy CB | ./rawdata-bearb/hour/alldata.CB.nt (directory) |
| 3. BEAR-B / Get the dataset / Granularity hour / Policy TB | ./rawdata-bearb/hour/alldata.TB.nq (file) |
| 3. BEAR-B / Get the dataset / Granularity hour / Policy CBTB | ./rawdata-bearb/hour/alldata.CBTB.nq (file) |
| 3. BEAR-B / Get the dataset / Granularity day / Policy IC | ./rawdata-bearb/day/alldata.IC.nt (directory) |
| 3. BEAR-B / Get the dataset / Granularity day / Policy CB | ./rawdata-bearb/day/alldata.CB.nt (directory) |
| 3. BEAR-B / Get the dataset / Granularity day / Policy TB | ./rawdata-bearb/day/alldata.TB.nq (file) |
| 3. BEAR-B / Get the dataset / Granularity day / Policy CBTB | ./rawdata-bearb/day/alldata.CBTB.nq (file) |

(*) relative to `/mnt/datastore/data/dslab/experimental/patch/`.

### Step2: download and expand queries

| Location in [the BEAR documentation](https://aic.ai.wu.ac.at/qadlod/bear.html) | destination (*) |
| ------------------------------------------------------------------------------ | --------------- |
| 2. BEAR-A / Get the queries / (12) | ./BEAR/queries_new/*.txt |
| 3. BEAR-B / Get the queries / (2) | ./BEAR/queries_bearb/*.txt |

(*) relative to `/mnt/datastore/data/dslab/experimental/patch/`.

### Step3: prepare the data

Copy all script files found in the `data-prepare-scripts` directory (relative to this README file) to `/mnt/datastore/data/dslab/experimental/patch/`.

For HDT:
- executa all `load*-hdt.sh` scripts one by one.

For jena:
- executa all `load*-jena.sh` scripts one by one.

Notes:
- the scripts `filter-cbtb.sh`, `filter-cb.sh` and `mv-cb.sh` were not investigated.

