########## 1) BUILD STAGE: Install deps, KC, Ostrich ##########
FROM ubuntu:22.04 AS install_ostrich

ENV DEBIAN_FRONTEND=noninteractive

# 1) Base dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git curl ca-certificates \
    cmake ninja-build wget \
    autoconf automake libtool pkg-config \
    zlib1g-dev liblzma-dev liblzo2-dev \
    libraptor2-dev \
    libserd-dev \
    libboost-iostreams-dev \
    python3 python3-pip python3-dev python3-venv \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# 2) Build Kyoto Cabinet
WORKDIR /tmp
RUN wget https://dbmx.net/kyotocabinet/pkg/kyotocabinet-1.2.79.tar.gz \
  && tar -xzf kyotocabinet-1.2.79.tar.gz \
  && mv kyotocabinet-1.2.79 kyotocabinet \
  && cd kyotocabinet \
  && ./configure --enable-lzo --enable-lzma \
  && make -j"$(nproc)" && make install && ldconfig \
  && rm -rf /tmp/kyotocabinet /tmp/kyotocabinet-1.2.79.tar.gz

# 3) Build Ostrich
WORKDIR /ostrich_eval
RUN git clone https://github.com/rdfostrich/ostrich

WORKDIR /ostrich_eval/ostrich
RUN if [ -d .git ]; then git submodule update --init --recursive; fi

RUN mkdir -p build \
  && cd build \
  && cmake -DCMAKE_BUILD_TYPE=Debug .. -Wno-deprecated \
  && make -j"$(nproc)"

# ---- WHAT WE NEED TO COPY IN FINAL STAGE ----
# - /usr/local/lib/* (Kyoto Cabinet, Ostrich libs)
# - /usr/local/include/* (headers if needed)
# - Ostrich build binaries: /ostrich_eval/ostrich/build/

# Clone starvers from Github
#FROM alpine/git:2.36.3 as base
#WORKDIR /
#RUN git clone https://github.com/GreenfishK/starvers.git

# Install starvers based on setup.py and modules based on requirements.txt
FROM python:3.11-slim AS install_python_modules
WORKDIR /

# Copy from local build context
COPY src/starvers /starvers_eval/starvers
COPY evaluation/starvers/scripts_dev/requirements.txt /starvers_eval
COPY evaluation/starvers/scripts_dev/eval_setup.toml /starvers_eval/configs/eval_setup.toml
COPY evaluation/starvers/raw_queries /starvers_eval/queries/raw_queries

# Copy from previous build stage
COPY --from=install_ostrich /usr/local/lib/ /usr/local/lib/
COPY --from=install_ostrich /usr/local/include/ /usr/local/include/
COPY --from=install_ostrich /ostrich_eval/ostrich/build/ /opt/ostrich/
COPY --from=install_ostrich /usr/lib/x86_64-linux-gnu/libboost_iostreams.so.1.74.0 /usr/lib/x86_64-linux-gnu/
COPY --from=install_ostrich /usr/lib/x86_64-linux-gnu/libboost_iostreams.so /usr/lib/x86_64-linux-gnu/


# copy from other images
COPY --from=stain/jena-fuseki:5.1.0 /jena-fuseki /jena-fuseki
COPY --from=eclipse-temurin:17.0.16_8-jdk /opt/java /opt/java/java17

COPY --from=ontotext/graphdb:10.5.0 /opt/graphdb /opt/graphdb
COPY --from=eclipse-temurin:11.0.21_9-jdk /opt/java /opt/java/java11

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

# Install basic unix/linux tools for the debian distribution
RUN apt-get update
RUN apt-get install -y bash coreutils procps grep sed curl bc wget 

# To suppress the GraphDB setlocale() warning and ensure UTF-8 everywhere
RUN apt-get update && apt-get install -y --no-install-recommends \
    locales liblzma5 liblzo2-2 zlib1g libraptor2-0 libserd-0-0 libboost-iostreams-dev \
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

# Copied from OSTRICH but not sure why this is needed
# EXPOSE 3000


# Docker knowledge
# RUN is an image build step, the state of the container after a RUN command will be committed to the container image. 
# A Dockerfile can have many RUN steps that layer on top of one another to build the image. 
# CMD is the command the container executes by default when you launch the built image. CMD is similar to entrypoint
# ENTRYPOINT parameters cannot be overriden ny command-line parameters in the `docker run` while with CMD this is possible.
# RUN you will usually find in Dockerfiles while CMD and ENTRYPOINT you will find in docker compose
# The last FROM command in the dockerfile creates the actual final image. 
# Images can be copied from previous stages with COPY --from=<path>