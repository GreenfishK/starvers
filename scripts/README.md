# Data processing and visualization 
## Building the RDF* dataset
We use the script build_tb_rdf_star_dataset.py to first compute change sets from the original dbpedia ICs provided on the [BEAR webpage](https://aic.ai.wu.ac.at/qadlod/bear.html). From the initial dataset 000001.nt and the computed change sets we build the RDF* datasets alldata.TB_star_flat.ttl and alldata.TB_star_hierarchical.ttl. Both datasets are annotated using RDF* syntax but the annotation style differs. In former dataset we use a "flat" style, meaning that we have as many data triples stored as there are annotations:
```
<<<s> <p> <o>>> <valid_from_predicate> <valid_from_date> 
<<<s> <p> <o>>> <valid_until_predicate> <valid_until_date> 
```
In the latter dataset we use a hierarchical approach and nest one specific triple as many times as there are annotation for that triple. This means that the data triple is uniquely represented in the whole dataset: 
```
<<<<<s> <p> <o>>> <valid_from_predicate> <valid_from_date>>> <valid_until_predicate> <valid_until_date> 
```


## Dataset verification
We created a notebook dataset_verification.ipynb where we checked the consistency between the provided ICs, change sets and quads-based TB dataset. 

## Load data
We use bash scripts to create the database repositories and import data. The original scripts from the [OSTRICH/BEAR github page](https://github.com/rdfostrich/BEAR/tree/master/src/common/data-prepare-scripts) where taken and modified to fit our directory structure and evaluation scenarios. Verify that the repositories have been created by looking into ~./BEAR/databases/\<vendor\>/\<policy\>. E.g. ~/.BEAR/databases/tdb-bearb-hour/tb_star_h. 

## Evaluation
In the evaluation directory we provide scripts for bulk query execution and query execution time measuring. A prerequisite to this is that the repositories have been created and the data has been loaded. The parameters can be set directly in the scripts to include certain archiving policies (ic, cb, tb, tb_star_f, tb_star_h, cbtb), query categories (mat, diff, ver) and set of queries as found in the [queries directory](https://github.com/GreenfishK/BEAR/tree/master/data/queries). After a script terminates a file with descriptive statistics for each set of queries should be written to the time/\<vendor\>/\<dataset\>/\<hostname-timestamp\> subdirectory of the [output directory](https://github.com/GreenfishK/BEAR/tree/master/data/output) 

# Original README
Find the original BEAR Readme [here](https://github.com/GreenfishK/BEAR/blob/master/scripts/README).
