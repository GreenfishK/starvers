# Requirements
Switching between runs: Upon clicking on a run, I want to see the steps of this run and how far the execution got. Currently, switching between runs and displaying the progress is not possible. only for the latest run it is visible.

Extra infos: Each of the seven steps is individual in its way regarding the inputs and outputs it contains. I want to have the following information displayed per step upon finish, which should only be visible upon clicking on a single step and expanding it to show the infos

Development principles 
* You should divide the code into css, html and scripts and not have everything in one html.

* Scope: starvers/evaluation/starvers/gui

* No need to update the dockerfile. The gui will be run in a container with: 
```
docker run -d --rm \
--env-file .env \
--name starvers-gui \
--network starvers_prod_net \
-v /mnt/data/starvers_eval:/starvers_eval/data \
starvers_eval:latest gui
```

## Download
* Which datasets are downloaded
    * Source of information: eval_setup.toml
* Which query sets are downloaded
    * Source of information for query set names: eval_setup.toml
    * Source of information for number of queries per set: a new function in download.sh that counts the files in the corresponding subdirectories
* How many versions a dataset has
    * Source of information: eval_setup.toml
* How big in size a dataset is
    * Source of information: a separate file datasets_meta.csv should be logged in RUN_DIR/output/logs/downloads/datasets_meta.csv where the size of all snapshots combined is recorded. The recording should be implemented in the download_data.sh

## Preprocess Data
* The essential substeps executed
    * skolemize_subject_blanks
    * skolemize_object_blanks
    * validate_and_comment_invalid_triples
    * extract_queries
    * exclude_queries
* name and version of the RDF validator: rdf4j parser and jena parser
    * source of information: rdfvalidator-1.0-jar-with-dependencies.jar. there are two files: apache-jena-libs-4.0.0.pom and rdf4j-client-3.7.4.pom from which the version can be extracted
* Number of blank nodes skolemized and number of invalid triples
    * Source of information: first three lines in each snapshot file at ${RUN_DIR}/rawdata/<dataset_name>/alldata.IC.nt/<snapshot.nt> which look like this:
        * "# invalid_lines_excluded: 0"
          "# skolemized_blank_nodes_in_object_position: 0"
          "# skolemized_blank_nodes_in_subject_position: 0"
* How many queries got excluded from the SciQA dataset for which reason
    * Source of information: csv file at ${RUN_DIR}output/logs/preprocess_data/excluded_queries.csv. count the total number of rows in the csv file minus the header grouped by the "reason" column

## Construct Datasets
* Which dataset variants are constructed: 
    * first IC + change sets (in the directory alldata.CB_computed.nt)
        * source of information: ${RUN_DIR}/rawdata/<dataset_name>/alldata.CB_computed.nt and ${RUN_DIR}/rawdata/<dataset_name>/alldata.IC.nt/1.nt or 000001.nt
    * named graph-based versioning dataset, BEAR's approach (alldata.TB_computed.nq)
        * source of information: ${RUN_DIR}/rawdata/<dataset_name>/
    * RDF-star dataset (alldata.TB_star_hierarchical.ttl)
        * source of information: ${RUN_DIR}/rawdata/<dataset_name>/
    * ICs all in one file (alldata.ICNG.trig)
        * source of information: ${RUN_DIR}/rawdata/<dataset_name>/
* How big each dataset variant is. calculate with "du", "-s", "-L", "--block-size=1M", "--apparent-size", str(path) where str(path) is either a file or directory (directory in the case of CBs)
* The versioning approach on RDF-level for the following dataset variants
    * first IC + change sets: no versioning at RDF-level but they are ingested and versioned by ostrich internally.
    * alldata.TB_computed.nq: 
        * """ex:s1 ex:p1 ex:o1 :v21_22_23_25 .
        :v21_22_23_25 owl:versionInfo "21" :versions .
        :v21_22_23_25 owl:versionInfo "22" :versions .
        :v21_22_23_25 owl:versionInfo "23" :versions .
        :v21_22_23_25 owl:versionInfo "25" :versions ."""
    * alldata.TB_star_hierarchical.ttl
        * << << s p o >> vers:valid_from creation_timestamp_literal >> vers:valid_until >> expiration_timestamp_literal .
    * alldata.ICNG.trig: 
        * GRAPH <http://starvers_eval/ic/v0> { triples  }
        GRAPH <http://starvers_eval/ic/v1> { triples  }
        
## Ingest
* The ingest information in ${RUN_DIR}/output/measurements/ingestion.csv displayed with tables and figures in a nice way. the header is: triplestore;policy;dataset;run;ingestion_time;raw_file_size_MiB;db_files_disk_usage_MiB
    * Source of information: ${RUN_DIR}/output/measurements/ingestion.csv
How many ingest runs: 10 (hardcoded)

## Construct queries
* How many queries are constructed for each versioning policy, dataset, and query set.
    * Source of information: ${RUN_DIR}/queries/final_queries . The subdirectories are: 
        * ic_sr_ng/bearb_day
        * ic_sr_ng/bearb_hour
        * ic_sr_ng/bearc
        * ic_sr_ng/orkg
        * ostrich/bearb_day
        * ostrich/bearb_hour
        * ostrich/bearc
        * ostrich/orkg
        * tb_sr_ng/bearb_day
        * tb_sr_ng/bearb_hour
        * tb_sr_ng/bearc
        * tb_sr_ng/orkg
        * tb_sr_rs/bearb_day
        * tb_sr_rs/bearb_hour
        * tb_sr_rs/bearc
        * tb_sr_rs/orkg
    Count the query files ending with .txt by traversing the directories recursively until you reach the bottom and find the .txt files
    * The policy codes ic_sr_ng, ostrich, tb_sr_ng, tb_sr_rs correspond to the dataset variants alldata.ICNG.trig, first IC + change sets, alldata.TB_computed.nq and alldata.TB_star_hierarchical.ttl, respectively.

## Evaluate
* How many queries are executed against each combination of triple_store, policy, dataset.
    * Source of information: eval_setup.toml, "evaluations" section

## Visualize
* Present the latex table at ${RUN_DIR}/output/tables/latex_table_results.tex


# Fixes
## 21.04.2026

### color scheme
use the same color scheme (TU Wien colors) as for the gui in https://github.com/GreenfishK/starvers/tree/main/src/starversserver/app/gui, which is a light mode color scheme

### download
The dataset details should show the size per dataset as recorded in $RUN_DIR/output/logs/download/datasets_meta.csv

The dataset details also should show the link per dataset, as found in the eval_setup.toml under [datasets] [datasets.<dataset_name>] download_link_snapshots=<link>

### preprocess data
RDF validators don't show the versions. This needs a fix

The skolemization summary does not show the dataset from which the triples where excluded and the skolemized blank nodes. it must show this info per dataset

### construct datasets
The table created for the construct datasets details does not show a column for the actual dataset prior to the VARIANT column

the RDF versioning approach should only be shown once per variant. maybe a table is not the ideal visualization but rather a section per variant where sizes are shown for each dataset and the versioning approach is shown as a sort of description.

### ingest
don't shot the full ingest table. Only show the avg per triple store, policy and dataset. also show the db size. use a visualization approach that does not have redundancies, i.e. repeated values. 


### construct queries
check what's going on with consturct queries as the details could not be loaded

### evaluate
throws a syntax error due to wrong retrieval of the policy, which does not exist as an attribute. this is how the data is structured:
[evaluations]
    [evaluations.graphdb]
        bearb_day   = ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]
        bearb_hour = ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]
        bearc      = ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]
        orkg       = ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]

    [evaluations.jenatdb2]
        bearb_day   = ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]
        bearb_hour = ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]
        bearc      = ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]
        orkg       = ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]

    [evaluations.ostrich]
        bearb_day   = ["ostrich"]
        bearb_hour = ["ostrich"]
        bearc      = ["ostrich"]
        orkg       = ["ostrich"]

the policies are the list values

### visualize
the latex table should be interpreted, not shown as a raw table

