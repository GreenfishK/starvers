Follow the instructions below to reproduce this experiment.
# Preliminaries
## Install docker 
install docker on [Ubuntu](https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository).

## Set experiment directory
Set the `data_storage` environment variable in the .env file to the snapshot data collected by starversserver (see src/starversserver/docker-prod/.env).


## Build docker container from image
Run the following command from the root directory of this project: `docker build --no-cache -t starversserver_eval -f starversserver_eval.Dockerfile .`

# Run experiment
Use the docker-compose services `compute` and `create_plots` to version the snapshots according to the RDF-star-based method used by starvers, compute metrics and create plots from the <RDF-dataset>_timings.csv files in the .