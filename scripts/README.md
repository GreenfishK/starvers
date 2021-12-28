# Data processing and visualization 
## Building the RDF* dataset
We use the script build_tb_rdf_star_dataset.py to first compute change sets from the original dbpedia ICs provided on the [BEAR webpage](https://aic.ai.wu.ac.at/qadlod/bear.html). From the initial dataset 000001.nt and the computed change sets we build the RDF* dataset alldata.TB_star.ttl. 

## Dataset verification
We created a notebook dataset_verification.ipynb where we checked the consistency between the provided ICs, change sets and quads-based TB dataset. 

## Load data
We use bash scripts to create the database repositories and import data. The original scripts from the [OSTRICH/BEAR github page](https://github.com/rdfostrich/BEAR/tree/master/src/common/data-prepare-scripts) where taken and modified to fit our directory structure and evaluation scenarios. Verify that the repositories have been created by looking into ~./BEAR/databases/<vendor>/<policy>. E.g. ~/.BEAR/databases/tdb-bearb-hour/tb_star. 

## Evaluation
In the evaluation directory we provide scripts for bulk query execution and query execution time measuring. A preliminary to this is that the repositories have been created and the data has been loaded. The parameters can be set directly in the scripts to include certain archiving policies (ic, cb, tb, tb_star, cbtb), query categories (mat, diff, ver) and set of queries as found in the [queries directory](https://github.com/GreenfishK/BEAR/tree/master/data/queries). After a script terminates a file with descriptive statistics for each set of queries should be written to the time/<vendor>/<dataset>/<hostname-timestamp> subdirectory of the [output directory](https://github.com/GreenfishK/BEAR/tree/master/data/output) 

# Original README
Find the original BEAR Readme [here](https://github.com/GreenfishK/BEAR/blob/master/scripts/README).
