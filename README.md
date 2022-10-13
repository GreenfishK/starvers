Follow the instructions below to reproduce this experiment.
# Install docker 
If you have docker installed already, continue with [Build docker images](https://github.com/GreenfishK/BEAR/blob/master/README.md#build-docker-images)
install docker on [Ubuntu](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository) or [other OS](https://docs.docker.com/get-docker/)
[get access as a non-root user](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user). Find the commands from that page bellow (07.12.2021).
```
(sudo groupadd docker)
sudo usermod -aG docker $USER 
newgrp docker
docker run hello-world
```

# Build docker images
Go to BEAR/rdfstarArchive and build the java rdfstoreQuery project with docker. The docker file uses a maven image to build and package the project with dependencies: 
```
docker build -t bear-rdfstarstores .
```
Go to the built jar, which should be in `/var/lib/docker/overlay2/<latest_build_id>/diff/target/rdfstoreQuery.0.8.jar` and add following line to the file `META-INF/services/org.eclipse.rdf4j.rio.RDFParserFactory` :
`org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory` \
The reason is that we have two dependencies - rdf4j-rio-turtle and rdf4j-rio-nquads - which are both implementations of the same interface. That is why the RDFParserFactory gets overriden with whichever is listed first in the dependencies in pom.xml (df4j-rio-turtle in our case). 

## Troubleshoot
Error1: “Docker does not have a release file”

Fix: Edit etc/apt/source.list.d/docker.list and set the release version to an Ubuntu version for which there is a docker release, e.g. “focal”: https://stackoverflow.com/questions/41133455/docker-repository-does-not-have-a-release-file-on-running-apt-get-update-on-ubun 

# Get data
Create a directory .BEAR in your home directory and copy the content of the BEAR/data directory including all sub-directories into ~/.BEAR . Give all user groups full permission (read, write, execute) to this directory and all subdirectories (chmod 777). 

## Download and prepare BEAR datasets
* Download the BEAR datasets from the [BEAR webpage](https://aic.ai.wu.ac.at/qadlod/bear.html) and copy them into 
    - ~/home/.BEAR/rawdata/beara
    - ~/home/.BEAR/rawdata/bearb/instant
    - ~/home/.BEAR/rawdata/bearb/hour
    - ~/home/.BEAR/rawdata/bearb/day
    - ~/home/.BEAR/rawdata/bearc
respectively.
* Extract the datasets from their .gz packages. Also extract all sub .gz packages.
* Rename allData.nq in ~/home/.BEAR/rawdata/bearb/hour to alldata.TB.nq.

## Queries
Queries are already provided in the [queries](https://github.com/GreenfishK/BEAR/tree/master/data/queries) directory. They were also downloaded from the [BEAR webpage](https://aic.ai.wu.ac.at/qadlod/bear.html).

## Create RDF-star datasets from BEAR's ICs and CBs
For each of the five datasets create the alldata.TB_star_flat.ttl and alldata_TB_star_hierarchical.ttl datasets by using the script provided [here](https://github.com/GreenfishK/BEAR/blob/master/scripts/build_tb_rdf_star_datasets.py). This script first computes changesets from the ICs and stores them into the alldata.CB_computed.nt sub-directory of the respective dataset directory. From the initial IC alldata.IC.nt/000001.nt and the change sets it creates two single-filed RDF-star-based TB dataset. These datasets include all snapshots/versions where each version is a set of triples with the same creation timestamp (valid_from). The timestamps do not reflect the original timestamps but are artificially constructed with one second difference between each version. Triples that were deleted and therefore not existent in a specific version anymore are annotated with a deletion timestamp (valid_until).


TODO: include following datasets:
* BEAR-A
* BEAR-B day
* BEAR-B instant
* BEAR-C



# Run the evaluation script 
Navigate to the directory BEAR/scripts/evaluation and run the following commands for each dataset:
```
./run-docker-rdf_star_triple_stores.sh beara 2>&1 | tee beara.log
./run-docker-rdf_star_triple_stores.sh bearb-instant 2>&1 | tee bearb-instant.log
./run-docker-rdf_star_triple_stores.sh bearb-hour 2>&1 | tee bearb-hour.log
./run-docker-rdf_star_triple_stores.sh bearb-day 2>&1 | tee bearb-day.log
./run-docker-rdf_star_triple_stores.sh bearc 2>&1 | tee bearc.log

```
Upon execution following new directories and files are created (marked with *):

```
home/.BEAR/  
.
.
.
└── output  
    └── *time
	    └──*beara
	       └── <hostname><system-timestamp>
	       	   └── dataset_infos.csv
	       	   └── ... TODO: include output files
	    └──*bearb-instant
	       └── <hostname><system-timestamp>
	       	   └── dataset_infos.csv
	       	   └── ... TODO: include output files
	    └──*bearb-hour
	       └── <hostname><system-timestamp>
	       	   └── dataset_infos.csv
	       	   └── time-tb-mat-lookup_queries_p.txt.csv
               └── time-tb-mat-lookup_queries_po.txt.csv
               └── time-tb_star_h-mat-lookup_queries_p.txt.csv
               └── time-tb_star_h-mat-lookup_queries_po.txt.csv
               └── time-tb_star_f-mat-lookup_queries_p.txt.csv
               └── time-tb_star_f-mat-lookup_queries_po.txt.csv
	    └──*bearb-day
	       └── <hostname><system-timestamp>
	       	   └── dataset_infos.csv
	       	   └── ... TODO: include output files
	    └──*bearc
	       └── <hostname><system-timestamp>
	       	   └── dataset_infos.csv
	       	   └── ... TODO: include output files

```


# Plot performance measurements
Use the [python script](https://github.com/GreenfishK/BEAR/blob/master/scripts/visualization.py) to plot the performance across all versions for different timestamp-based archiving policies, query categories and query sets.

# Contact
filip.kovacevic@tuwien.ac.at

# Original OSTRICH README
see [https://github.com/rdfostrich/BEAR](https://github.com/rdfostrich/BEAR)
