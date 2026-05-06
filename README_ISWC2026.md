## Starvers Evaluation
Starvers is evaluated using an automated pipeline that has three main input parameters:
* triple stores
* dataset
* versioning policy

The docker container needs first to be built using the command
```bash
docker build -t starvers_eval:gui -f starvers.eval.Dockerfile .
```
**!important!**: The directory /mnt/data_local/starvers_eval needs to have at least 350GB. All data from the run are automatically written to this directory.

Then, the whole pipeline can be executed using the command in Section [Run the full pipeline](#run-the-full-pipeline).

### Pipeline Steps

| # | Step | Docker-Compose Service |
|---|------|------------------------|
| 1 | download | `download` |
| 2 | preprocess_data | `preprocess_data` |
| 3 | construct_datasets | `construct_datasets` |
| 4 | ingest | `ingest` |
| 5 | construct_queries | `construct_queries` |
| 6 | evaluate | `evaluate` |
| 7 | visualize | `visualize` |

Each step can be run in isolation, or the full pipeline can be executed to run all steps consecutively. Parameters, such as the triple stores, versioning policies, and datasets to evaluate are read from the .env file.


### Run the full pipeline 

```bash
docker run -d --rm \
--name starvers_eval \
--env-file .env \
--ulimit nofile=1048576:1048576 \
--add-host Starvers:127.0.0.1 \
-v /mnt/data_local/starvers_eval:/starvers_eval/data \
-v /mnt/data_local/starvers_eval/tmp:/tmp \
starvers_eval:latest run all
```

### Run a single step

```bash
docker run -d --rm \
--name starvers_eval \
--env-file .env \
--ulimit nofile=1048576:1048576 \
--add-host Starvers:127.0.0.1 \
-v /mnt/data_local/starvers_eval:/starvers_eval/data \
-v /mnt/data_local/starvers_eval/tmp:/tmp \
starvers_eval:latest run step download
```

### Run from a specific step

```bash
docker run -d --rm \
--name starvers_eval \
--env-file .env \
--ulimit nofile=1048576:1048576 \
--add-host Starvers:127.0.0.1 \
-v /mnt/data_local/starvers_eval:/starvers_eval/data \
-v /mnt/data_local/starvers_eval/tmp:/tmp \
starvers_eval:latest run from construct_datasets
```

### Continue a previously interrupted run

The orchestrator records each step's start time, end time, and status in
`/mnt/data_local/starvers_eval/<timestamp>/execution.csv`. If a run was interrupted
or a step failed, resume from the last unfinished step:

```bash
docker run -d --rm \
--name starvers_eval \
--env-file .env \
--ulimit nofile=1048576:1048576 \
--add-host Starvers:127.0.0.1 \
-v /mnt/data_local/starvers_eval:/starvers_eval/data \
-v /mnt/data_local/starvers_eval/tmp:/tmp \
starvers_eval:latest continue
```

### List all runs

```bash
docker run -d --rm \
--name starvers_eval \
--env-file .env \
-v /mnt/data_local/starvers_eval:/starvers_eval/data \
starvers_eval:latest list
```

### Delete old runs

```bash
# Delete all run directories created before 2026-01-01 00:00:00
docker run -d --rm \
--name starvers_eval \
--env-file .env \
-v /mnt/data_local/starvers_eval:/starvers_eval/data \
starvers_eval:latest delete --older-than 20260101T000000
```
