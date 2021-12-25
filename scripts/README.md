# Building the RDF* dataset
We use the script build_tb_rdf_star_dataset.py to first compute change sets from the original dbpedia ICs provided on the [BEAR webpage](https://aic.ai.wu.ac.at/qadlod/bear.html). From the initial dataset 000001.nt and the computed change sets we build the RDF* dataset alldata.TB_star.ttl. 

# Dataset verification
We created a notebook dataset_verification.ipynb where we checked the consistency between the provided ICs, change sets and quads-based TB dataset. 

# Load data
We use bash scripts to create the database repositories and import data. The original scripts from the [OSTRICH/BEAR github page](https://github.com/rdfostrich/BEAR/tree/master/src/common/data-prepare-scripts) where taken and modified to fit our directory structure and evaluation scenarios.

# Original README
Find the original BEAR Readme [here](https://github.com/GreenfishK/BEAR/blob/master/scripts/README).
