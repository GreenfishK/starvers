# Changes
Two files run_starvers_eval.py and run_starversserver_eval.py which orchestrate the respective evaluation pipelines. 

## Starvers Evaluation
Currently, the docker-compose file starvers.eval.compose.yml uses services that represent individual execution steps from the evaluation pipeline, such as download, preprocess_data, construct_datasets, and so on. It is possible to execute each of them individually, although they are dependent on each other. E.g. I cannot preprocess data if it has not been downloaded first. In the run_starvers_eval.py there should be a control flow and logic that has the following features:
* executing individual steps
* executing the whole pipeline
* executing everything from a specific step
* an interface for the three functionalities above, so that I can select the function I want to execute via a docker command that calls run_starvers_eval.py with specific parameters for the selection of the function.
* A timestamping for the runs so that every run gets recorded under the (host) directory /mnt/data/starvers_eval/YYYYMMDDThh:mm:ss.sss
* Functionality that records the start and end time of every execution step in a separate csv file under /mnt/data/starvers_eval/YYYYMMDDThh:mm:ss.sss/execution.csv
* Functionality for the user to continue the last execution, in case it failed or stopped. This is simply judged by the execution.csv table. The idea is that if a step did not finish, it is visible in this table and "continue" means to start from the last unfinished step.
* A function that lets the user delete previous runs, i.e. the whole directories /mnt/data/starvers_eval/YYYYMMDDThh:mm:ss.sss that are older than the given timestamp

The docker-compose file starvers.eval.compose.yml uses infrastructure-level variables from the .env file for volume mounts. This is only something to consider during the execution via docker.

A documentation for the docker-based execution with all the different possibilities is needed, including the full explicit command for executing the whole pipeline including the volume binds and port mappings, essentially the parameters currently used in the starvers.eval.compose.yml services for the individual steps. This documentation will be placed in the README.md.

### GUI
A gui for monitoring the current and previous executions that should be reachable via https://starvers.ec.tuwien.ac.at/evaluation/starvers
The necessary additions must be placed in the following files
* nginx file: nginx_default.conf
* A new directory under evaluation/starvers/gui
* Docker commands for the README.md for executing the GUI in a container, thereby making the subpage under https://starvers.ec.tuwien.ac.at/evaluation/starvers accessible

## Starversserver Evaluation
The same functionality should be implemented as for Starvers Evaluation in the run_starversserver_eval.py  effectively replacing the docker-compose file starversserver.eval.compose.yml. Additionally, the environment file starversserver.eval.env should also be considered.

The docker-compose file starversserver.eval.compose.yml uses infrastructure-level variables from the .env file as well as runtime variabls from starversserver.eval.env which should also be considered.

### GUI
A gui for monitoring the current and previous executions that should be reachable via https://starvers.ec.tuwien.ac.at/evaluation/starversserver
The necessary additions must be placed in the following files
* nginx file: nginx_default.conf
* A new directory under evaluation/starversserver/gui
* Docker commands for the README.md for executing the GUI in a container, thereby making the subpage under https://starvers.ec.tuwien.ac.at/evaluation/starversserver accessible