# Clone starvers from Github
FROM alpine/git:2.36.3 as clone_starvers
WORKDIR /
RUN git clone https://github.com/GreenfishK/starvers.git

# Create directories
RUN mkdir -p /starvers_eval/databases
RUN mkdir -p /starvers_eval/output/logs
RUN mkdir -p /starvers_eval/output/measurements
RUN mkdir -p /starvers_eval/output/figures
RUN mkdir -p /starvers_eval/rawdata
RUN mkdir -p /starvers_eval/configs

RUN mkdir -p /starvers_eval/scripts/1_get_and_prepare_data
RUN mkdir -p /starvers_eval/scripts/2_load_data
RUN mkdir -p /starvers_eval/scripts/3_generate_queries
RUN mkdir -p /starvers_eval/scripts/4_evaluation
RUN mkdir -p /starvers_eval/scripts/5_visualization

# Copy raw queries and scripts
COPY /data/queries/raw_queries /starvers_eval/queries/raw_queries
COPY scripts_dev/1_get_and_prepare_data/build_tb_rdf_star_datasets.py /starvers_eval/scripts/1_get_and_prepare_data
COPY scripts_dev/1_get_and_prepare_data/data_corrections.py /starvers_eval/scripts/1_get_and_prepare_data
COPY scripts_dev/1_get_and_prepare_data/download_data.sh /starvers_eval/scripts/1_get_and_prepare_data

COPY scripts_dev/2_load_data/configs /starvers_eval/scripts/2_load_data/configs
COPY scripts_dev/2_load_data/create_and_load_triplestores.sh /starvers_eval/scripts/2_load_data

COPY scripts_dev/3_generate_queries /starvers_eval/scripts/3_generate_queries

COPY scripts_dev/4_evaluation/evaluate.py /starvers_eval/scripts/4_evaluation
COPY scripts_dev/4_evaluation/evaluate.sh /starvers_eval/scripts/4_evaluation

# TODO: add visualization

FROM stain/jena-fuseki:4.0.0 as install_jena
WORKDIR /
COPY --from=clone_starvers /starvers_eval /starvers_eval
COPY --from=clone_starvers /starvers /starvers

FROM ontotext/graphdb:9.11.2-se as install_graphdb
WORKDIR /
COPY --from=install_jena /starvers_eval /starvers_eval
COPY --from=install_jena /starvers /starvers
COPY --from=install_jena /fuseki/. /fuseki
COPY --from=install_jena /jena-fuseki /jena-fuseki
COPY --from=install_jena /usr/local/openjdk-11 /usr/local/openjdk-11

COPY scripts_dev/2_load_data/configs/graphdb.license /opt/graphdb/home/conf/

# Install starvers (build) and modules from requirements.txt
FROM python:3.8.15-slim as install_python
COPY --from=install_graphdb /starvers /starvers
COPY --from=install_graphdb /starvers_eval /starvers_eval
# COPY --from=install_graphdb /fuseki /fuseki
COPY --from=install_graphdb /jena-fuseki /jena-fuseki
COPY --from=install_graphdb /usr/local/openjdk-11 /usr/local/openjdk-11
COPY --from=install_graphdb /opt /opt

RUN python3 -m venv /starvers_eval/python_venv
RUN . /starvers_eval/python_venv/bin/activate
WORKDIR /starvers
RUN /starvers_eval/python_venv/bin/python3 -m pip install .
COPY scripts_dev/requirements.txt /starvers_eval
WORKDIR /starvers_eval
RUN /starvers_eval/python_venv/bin/python3 -m pip install -r requirements.txt

FROM python:3.8.15-slim as final_stage
# copy only necessary directories
COPY --from=install_python /starvers_eval /starvers_eval
# COPY --from=install_python /fuseki /fuseki
COPY --from=install_python /jena-fuseki /jena-fuseki
COPY --from=install_python /usr/local/openjdk-11 /usr/local/openjdk-11
COPY --from=install_python /opt /opt

# Install GNU basic calculator
RUN apt-get update
RUN apt-get install bc -y

## Set graphdb environment variables
ENV GDB_JAVA_OPTS='\
-Xmx5g -Xms5g \
-Dgraphdb.home=/opt/graphdb/home \
-Dgraphdb.workbench.importDirectory=/opt/graphdb/home/graphdb-import \
-Dgraphdb.workbench.cors.enable=true \
-Denable-context-index=true \
-Dentity-pool-implementation=transactional \
-Dhealth.max.query.time.seconds=60 \
-Dgraphdb.append.request.id.headers=true \
-Dreuse.vars.in.subselects=true'


# Docker knowledge
# RUN RUN is an image build step, the state of the container after a RUN command will be committed to the container image. 
# A Dockerfile can have many RUN steps that layer on top of one another to build the image. 
# CMD is the command the container executes by default when you launch the built image. CMD is similar to entrypoint
# ENTRYPOINT parameters cannot be overriden ny command-line parameters in the `docker run` while with CMD this is possible.
# RUN you will usually find in Dockerfiles while CMD and ENTRYPOINT you will find in docker compose
# The last FROM command in the dockerfile creates the actual final image. 
# Images can be copied from previous stages with COPY --from=<path>