Reproduce experiment
==============
Follow the instructions below to reproduce this experiment.
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

## Build docker images
Go to BEAR/src/Jena_TDB and build the java Jena-TDB project with docker. The docker file uses a maven image to build and package the project with dependencies: 
```
docker build -t bear-jena .
```

### Troubleshoot
Error1: “Docker does not have a release file”

Fix: Edit etc/apt/source.list.d/docker.list and set the release version to an Ubuntu version for which there is a docker release, e.g. “focal”: https://stackoverflow.com/questions/41133455/docker-repository-does-not-have-a-release-file-on-running-apt-get-update-on-ubun 

## Get data
Create the local data directories for this experiment. Download the datasets & queries and compute the changesets from the ICs to build the RDF* dataset. See [here](https://github.com/GreenfishK/BEAR/tree/master/data).

## Run the evaluation script 
Run the built jar file via docker using our [bash script](https://github.com/GreenfishK/BEAR/blob/master/scripts/evaluation) to evaluate the data ingestion and query performance with Jena for the three timestamp-based archiving policies (tb (=named graphs), tb\_rdf\_star\_h, tb\_rdf\_star\_f) and materialization queries. As opposed to the original BEAR script, the TDB database files are now created during runtime via the Java TDBLoader API and do not need to be manually loaded via CLI. If you still wish to manually create and load the TDB store you can use our scripts provided [here](https://github.com/GreenfishK/BEAR/tree/master/scripts/load_data/Archiv). The raw RDF(*) datasets are taken from the location which is provided as parameter in the "run docker" script mentioned above. The TDB datasets are then mounted onto Jena's fuseki server, which is also created during runtime. This enables the queries to be executed via http requests, as opposed to direct java invocation as used in the original BEAR-Jena java project. \
Note that we are not evaluating IC or CB policies. This can, nevertheless, still be done with our script by passing the corresponding parameters. However, one needs to load the TDB datasets manually first and direct java invocation will be used for these policies, as in the original BEAR and OSTRICH framework.

## Plot performance measurements
Use the [python script](https://github.com/GreenfishK/BEAR/blob/master/scripts/plot_tb_and_tb_star.py) to plot the performance across all versions for different archiving policies, query categories and query sets.

Contact
==============
filip.kovacevic@tuwien.ac.at

# Original OSTRICH README
see README_orig.txt
