Follow the instructions below to reproduce this experiment.
# Preliminaries
## Install docker 
If you have docker installed already, continue with [Build docker images](https://github.com/GreenfishK/BEAR/blob/master/README.md#build-docker-images)
install docker on [Ubuntu](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) or [other OS](https://docs.docker.com/get-docker/)
[get access as a non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user). Find the commands from that page bellow (07.12.2021).
```
(sudo groupadd docker)
sudo usermod -aG docker $USER 
newgrp docker
docker run hello-world
```

## Create experiment directory
Create a directory `/mnt/data/starvers_eval` and make sure that docker can write to it by changing the privileges. This is the default host directory used by the docker-compose services. If you wish to change that, you can so in the .env file.

## Build docker container from image
Run the following command from the root directory of this project: `docker build --no-cache -t starvers_eval:latest -f starvers_eval.Dockerfile.`

# Run experiment
The experiment can be fully run by executing the following 7 docker-compose services one-by-one. This process is not automated as some individual steps need a considerable amount of time to finish. We want to make sure that each of them runs through and repeat them otherwise.
* `docker-compose run download`: Downloads the BEAR datasets and query sets
* `docker-compose run clean_raw_datasets`: Cleans the datasets by skolomizing blank nodes and commenting out invalid triples.
* `docker-compose run construct_datasets`: For each raw dataset (BEARB_day, BEARB_hour, BEARC) it constructs the change sets, the StarVers RDF-star-based dataset, a dataset with all ICs stored into named graphs and a dataset with all change sets stored into named graphs. It also measures the execution time of the insert and outdate functions from the [StarVers](https://github.com/GreenfishK/starvers) API.
* `docker-compose run ingest`: Loads all 12 constructed datasets from the previous step into GraphDB and Jena TDB2, respectively.
* `docker-compose run construct_queries`: Constructs the evaluation queries from the raw queries that have been downloaded in the first step. In total there should be 456.584 queries. 
    * 4 policies/dataset variants x 89 versions x 82 raw queries for BERAB_day
    * 4 policies/dataset variants x 1299 versions x 82 raw queries for BERAB_hour 
    * 4 policies/dataset variants x 33 versions x 10 raw queries for BERAB_day.
    * = 456.584
* `docker-compose run evaluate`: Runs the queries against the repositories and measures the execution time.
* `docker-compose run visualize`: Creates 7 figures with plots for query performance, dataset sizes & ingestion and update performance. These 7 figures are provided in the [output/figures](https://github.com/GreenfishK/starvers_eval/tree/master/output/figures) directory of this project. It also creates two LaTeX tables with query performance and dataset sizes.

# Dataset infos
**ORKG**

| Index | Dataset Version                 |
| ----- | ------------------------------- |
| 1     | orkg_20250612-175602_123.raw.nt |
| 2     | orkg_20250619-175627_677.raw.nt |
| 3     | orkg_20250626-175627_681.raw.nt |
| 4     | orkg_20250703-175702_124.raw.nt |
| 5     | orkg_20250710-175927_272.raw.nt |
| 6     | orkg_20250717-180528_763.raw.nt |
| 7     | orkg_20250724-153328_083.raw.nt |
| 8     | orkg_20250731-104002_735.raw.nt |
| 9     | orkg_20250807-201329_176.raw.nt |
| 10    | orkg_20250814-163412_451.raw.nt |
| 11    | orkg_20250821-125832_692.raw.nt |
| 12    | orkg_20250828-124726_863.raw.nt |
| 13    | orkg_20250904-130136_081.raw.nt |
| 14    | orkg_20250911-135336_629.raw.nt |
| 15    | orkg_20250918-153919_872.raw.nt |
| 16    | orkg_20250925-163726_720.raw.nt |
| 17    | orkg_20251002-173610_231.raw.nt |
| 18    | orkg_20251009-182702_732.raw.nt |
| 19    | orkg_20251016-191959_784.raw.nt |
| 20    | orkg_20251023-201131_188.raw.nt |
| 21    | orkg_20251030-211417_191.raw.nt |
| 22    | orkg_20251106-220626_525.raw.nt |
| 23    | orkg_20251113-224923_158.raw.nt |
| 24    | orkg_20251120-235816_435.raw.nt |
| 25    | orkg_20251127-005136_883.raw.nt |
| 26    | orkg_20251204-013528_440.raw.nt |
| 27    | orkg_20251211-023605_696.raw.nt |
| 28    | orkg_20251218-032843_047.raw.nt |
| 29    | orkg_20251225-042202_130.raw.nt |
| 30    | orkg_20251230-045735_730.raw.nt |
| 31    | orkg_20260110-052159_232.raw.nt |
| 32    | orkg_20260115-060408_384.raw.nt |
| 33    | orkg_20260122-070450_488.raw.nt |
| 34    | orkg_20260129-081055_236.raw.nt |


# Contact
filip.kovacevic@tuwien.ac.at
