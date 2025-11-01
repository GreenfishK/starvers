# Clone starvers from Github
#FROM alpine/git:2.36.3 as base
#WORKDIR /
#RUN git clone https://github.com/GreenfishK/starvers.git

# Install starvers based on setup.py and modules based on requirements.txt
FROM python:3.11-slim AS install_python_modules
WORKDIR /

COPY src/starvers /starvers_eval/starvers
COPY evaluation/starvers/scripts_dev/requirements.txt /starvers_eval
COPY evaluation/starvers/scripts_dev/eval_setup.toml /starvers_eval/configs/eval_setup.toml
COPY evaluation/starvers/raw_queries /starvers_eval/queries/raw_queries

# Install requirements for evaluation
WORKDIR /starvers_eval
RUN pip install --no-cache-dir -r requirements.txt

# Create /starvers_eval directories
RUN mkdir -p /starvers_eval/databases
RUN mkdir -p /starvers_eval/output/logs
RUN mkdir -p /starvers_eval/output/measurements
RUN mkdir -p /starvers_eval/output/result_sets
RUN mkdir -p /starvers_eval/output/figures
RUN mkdir -p /starvers_eval/rawdata
RUN mkdir -p /starvers_eval/scripts/1_download
RUN mkdir -p /starvers_eval/scripts/2_clean_raw_datasaets
RUN mkdir -p /starvers_eval/scripts/3_construct_datasets
RUN mkdir -p /starvers_eval/scripts/4_ingest
RUN mkdir -p /starvers_eval/scripts/5_construct_queries
RUN mkdir -p /starvers_eval/scripts/6_evaluate
RUN mkdir -p /starvers_eval/scripts/7_visualize

# copy from other images
COPY --from=stain/jena-fuseki:5.1.0 /jena-fuseki /jena-fuseki
COPY --from=stain/jena-fuseki:5.1.0 /opt/java /opt/java/java17 

COPY --from=ontotext/graphdb:10.5.0 /opt/graphdb /opt/graphdb
COPY --from=eclipse-temurin:11.0.21_9-jdk /opt/java /opt/java/java11

# Install basic unix/linux tools for the debian distribution
RUN apt-get update
RUN apt-get install -y bash coreutils procps grep sed curl bc wget 

# To suppress the GraphDB setlocale() warning and ensure UTF-8 everywhere
RUN apt-get update && apt-get install -y --no-install-recommends locales \
    && sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

## Set graphdb environment variables
ENV GDB_JAVA_OPTS='\
-Xmx45g -Xms45g \
-Dgraphdb.dist=/opt/graphdb/dist \
-Dgraphdb.home.work=/tmp/graphdb/work \
-Dgraphdb.workbench.importDirectory=/opt/graphdb/home/graphdb-import \
-Dgraphdb.workbench.cors.enable=true \
-Denable-context-index=true \
-Dentity-pool-implementation=transactional \
-Dhealth.max.query.time.seconds=60 \
-Dgraphdb.append.request.id.headers=true \
-Dreuse.vars.in.subselects=true'

# Set jenatdb2 environment variables
ENV FUSEKI_HOME=/jena-fuseki
ENV JVM_ARGS='-Xms45g -Xmx45g'
ENV ADMIN_PASSWORD=starvers

# For module imports relative to PYTHONPATH
ENV PYTHONPATH=/starvers_eval

# Docker knowledge
# RUN is an image build step, the state of the container after a RUN command will be committed to the container image. 
# A Dockerfile can have many RUN steps that layer on top of one another to build the image. 
# CMD is the command the container executes by default when you launch the built image. CMD is similar to entrypoint
# ENTRYPOINT parameters cannot be overriden ny command-line parameters in the `docker run` while with CMD this is possible.
# RUN you will usually find in Dockerfiles while CMD and ENTRYPOINT you will find in docker compose
# The last FROM command in the dockerfile creates the actual final image. 
# Images can be copied from previous stages with COPY --from=<path>