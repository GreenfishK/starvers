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

# Contact
filip.kovacevic@tuwien.ac.at
