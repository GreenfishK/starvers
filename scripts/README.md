# Data processing and visualization 
## Building the RDF-star dataset
We use the script build_tb_rdf_star_dataset.py to first compute change sets from the original dbpedia ICs provided on the [BEAR webpage](https://aic.ai.wu.ac.at/qadlod/bear.html). From the initial dataset 000001.nt and the computed change sets we build the RDF-star datasets alldata.TB_star_flat.ttl and alldata.TB_star_hierarchical.ttl. Both datasets are annotated using RDF-star syntax but the annotation style differs. In former dataset we use a "flat" style, meaning that we have as many data triples stored as there are annotations:
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
We use bash scripts, similar to [OSTRICH/BEAR github page](https://github.com/rdfostrich/BEAR/tree/master/src/common/data-prepare-scripts), to create the database repositories and import data. Verify that the repositories have been created by looking into ~./BEAR/databases/\<vendor\>/\<policy\>. E.g. ~/.BEAR/databases/tdb-bearb-hour/tb_star_h. 

## Evaluation
In the evaluation directory we provide a script for bulk query execution and query execution time measuring. The parameters can be set directly in the scripts to include certain archiving policies (ic, cb, tb, tb_star_f, tb_star_h, cbtb), query categories (mat, diff, ver) and set of queries as found in the [queries directory](https://github.com/GreenfishK/BEAR/tree/master/data/queries). After a script terminates a file with descriptive statistics for each set of queries should be written to the time/\<vendor\>/\<dataset\>/\<hostname-timestamp\> subdirectory of the [output directory](https://github.com/GreenfishK/BEAR/tree/master/data/output) \

The TDB database files are created during runtime via the Java TDBLoader API. The TDB datasets are then mounted onto Jena's fuseki server which is created during runtime. This enables the queries to be executed via http requests. To load the raw dataset files into an embedded GraphDB repository we use a Sail configuration file together with a couple of java constructors and method calls. \

Note that we are not evaluating IC or CB policies as for [Data Citation](https://rd-alliance.org/system/files/documents/RDA-DC-Recommendations_151020.pdf), which is our main motivation for this project, timestamped-based versioning is recommended. This can, nevertheless, still be done with our script by passing the corresponding parameters. However, one needs to load the TDB datasets manually first and direct java invocation will be used for these policies, as in the original BEAR and OSTRICH framework.

