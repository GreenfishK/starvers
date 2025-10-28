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
Create a directory `/mnt/data/starvers_eval_ostrich` and make sure that docker can write to it by changing the privileges. This is the default host directory used by the docker-compose services. If you wish to change that, you can so in the .env file.

## Build docker container from image
Run the following command in the root directory of this project to build the OSTRICH container: `docker build -t ostrich:latest -f Dockerfile.ostrich .`
Run the following command in the root directory of this project to build the starvers_eval container: `docker build -t starvers_eval .`

# Run experiment
The experiment is split up in two parts. The first part is the starvers_eval evaluation. This is a mirror of the `https://github.com/GreenfishK/starvers_eval` repostory and can be executed in the same way. A copy of the README.md for starvers_eval is pasted at the very bottom.
The second part is a evaluation of the BEAR dataset on the OSTRICH triplestore.

## OSTRICH evaluation
The experiment can be fully run by executing the following 7 docker-compose services one-by-one. This process is not automated as some individual steps need a considerable amount of time to finish. We want to make sure that each of them runs through and repeat them otherwise.
* `docker-compose run download`: Downloads the BEAR datasets and query sets (Same as for starvers_eval and can be reused)
* `docker-compose run clean_raw_datasets`: Cleans the datasets by skolomizing blank nodes and commenting out invalid triples. (Same as for starvers_eval and can be reused)
* `docker-compose run ostrich_construct_datasets`: For each raw dataset (BEARB_day, BEARB_hour, BEARC) it constructs the datasets for OSTRICH in the expected format.
* `docker-compose run ostrich_ingest`: Ingests all constructed datasets into an own OSTRICH store which are mounted onto the host. These stores can be reused.
* `docker-compose run ostrich_construct_queries`: Constructs the evaluation queries from the raw queries that have been downloaded in the first step in two seperate formats. 
    * `triple-pattern`: Construct triple pattern queries for the lookup BEAR queries in the expected format of OSTRICH
    * `SPARQL`: Construct SPARQL queries for all downloaded BEAR queries in the expected format of OSTRICH
* `docker-compose run ostrich_evaluate`: Runs the SPARQL queries against the OSTRICH stores and measures the execution time. Be aware, that this step is only executable for one dataset at a time at the moment. The dataset has to be configured in the .env file and in the docker-compose.yaml file in the `comunica-endpoint` service. To be specific, line 276 in the docker-compose.yaml file has to be adapted to the corresponding dataset, which is getting evaluated
* `docker-compose run ostrich_evaluate_basic`: Runs the triple-pattern queries against the OSTRICH stores and measures the execution time.
* `docker-compose run ostrich_visualize`: Creates 4 plots for every dataset. Ingestion Time, VM query performance, DM query peformance and VQ query performance. The plots only show the performance of the triple pattern queries.
* `docker-compose run visualize`: This command executes the normal visualization for starvers_eval. When setting the Environment Variable `OSTRICH` to 1 in line 198 of the docker-compose.yaml file, it will also include the OSTRICH measurements into the plots and provide a comparison between the different RDF archives. This step needs a successfull starvers_eval evaluation before being executed.


## starvers_eval evaluation
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
