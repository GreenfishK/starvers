#########################################################
# Stage 1: ostrich_build
# Builds Ostrich (native C++ triple store) and the
# Comunica SPARQL engine with Node.js bindings.
#########################################################
FROM node:14-bullseye AS ostrich_build

RUN apt-get update && apt-get install -y \
    build-essential python3 cmake ninja-build clang \
    libc++-dev libc++abi-dev libboost-iostreams-dev \
    git ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/.cmake-js && chmod 700 /root/.cmake-js

ENV CC=clang
ENV CXX=clang++

RUN git clone --branch dev --recurse-submodules \
    https://github.com/dkw-aau/ostrich-node.git /opt/ostrich-node

RUN cd /opt/ostrich-node && ./install-kc-ci.sh

# Build and register the package globally via yarn link
RUN cd /opt/ostrich-node \
    && yarn install --ignore-engines \
    && yarn link

RUN git clone https://github.com/rdfostrich/ostrich /opt/ostrich

RUN cd /opt/ostrich \
    && git submodule update --init --recursive \
    && mkdir -p build && cd build \
    && cmake -DCMAKE_BUILD_TYPE=Debug .. -Wno-deprecated \
    && make -j"$(nproc)"

RUN git clone https://github.com/dkw-aau/comunica-feature-versioning.git \
    /opt/comunica-feature-versioning

# yarn link ostrich-bindings creates a symlink back to /opt/ostrich-node,
# preserving the relative path that boost-lib's CMakeLists.txt depends on
RUN cd /opt/comunica-feature-versioning \
    && yarn link ostrich-bindings \
    && yarn install --ignore-engines

# NODE_PATH needed because yarn link uses ~/.config/yarn/link which isn't
# on Node's default resolution path when require() is called outside the
# Comunica package directory
ENV NODE_PATH=/usr/local/share/.config/yarn/link:/opt/comunica-feature-versioning/node_modules

RUN node -e "require('ostrich-bindings'); console.log('Ostrich bindings OK')"

#########################################################
# Stage 2: fuseki_base
# Pulls Jena Fuseki and its required JDK (Java 17).
#########################################################
FROM scratch AS fuseki_base

COPY --from=stain/jena-fuseki:5.1.0       /jena-fuseki  /jena-fuseki
COPY --from=eclipse-temurin:17.0.16_8-jdk /opt/java     /opt/java/java17


#########################################################
# Stage 3: graphdb_base
# Pulls GraphDB and its required JDK (Java 11).
#########################################################
FROM scratch AS graphdb_base

COPY --from=ontotext/graphdb:10.5.0         /opt/graphdb  /opt/graphdb
COPY --from=eclipse-temurin:11.0.21_9-jdk   /opt/java     /opt/java/java11


#########################################################
# Stage 4: oxigraph_build
# Compiles oxigraph-cli from source using Cargo/Rust.
#########################################################
FROM python:3.11-slim-bookworm AS oxigraph_build

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl build-essential pkg-config libssl-dev \
    clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -o get_cargo.sh https://sh.rustup.rs -s \
    && chmod +x get_cargo.sh \
    && ./get_cargo.sh -y --default-toolchain stable \
    && rm get_cargo.sh

RUN $HOME/.cargo/bin/cargo install oxigraph-cli

#########################################################
# Stage 5: python_base
# Installs the Python evaluation framework (starvers)
# and all pip dependencies. No triple store code here.
#########################################################
FROM python:3.11-slim-bookworm AS python_base

WORKDIR /starvers_eval

COPY src/starvers /starvers_eval/starvers
COPY evaluation/starvers/scripts_dev/requirements.txt            .
COPY evaluation/starvers/scripts_dev/eval_setup.toml  /starvers_eval/configs/eval_setup.toml

RUN pip install --no-cache-dir -r requirements.txt


#########################################################
# Stage 6: final
# Assembles all stages into the runtime image.
# Only this stage ships; earlier stages are cache only.
#########################################################
FROM python:3.11-slim-bookworm AS final

# ── Ostrich / Node / Comunica ────────────────────────
COPY --from=ostrich_build /usr/local/lib/ \
                          /usr/local/lib/
COPY --from=ostrich_build /usr/local/include/ \
                          /usr/local/include/
COPY --from=ostrich_build /usr/local/bin/node \
                          /usr/local/bin/node
COPY --from=ostrich_build /usr/local/lib/node_modules \
                          /usr/local/lib/node_modules
COPY --from=ostrich_build /usr/local/share/.config/yarn \ 
                          /usr/local/share/.config/yarn
COPY --from=ostrich_build /usr/lib/x86_64-linux-gnu/libboost_iostreams.so.1.74.0 \
                          /usr/lib/x86_64-linux-gnu/
COPY --from=ostrich_build /usr/lib/x86_64-linux-gnu/libboost_iostreams.so \
                          /usr/lib/x86_64-linux-gnu/
COPY --from=ostrich_build /opt/comunica-feature-versioning \
                          /opt/comunica-feature-versioning
COPY --from=ostrich_build /opt/ostrich-node \
                          /opt/ostrich-node
COPY --from=ostrich_build /opt/ostrich/build/ \
                          /opt/ostrich/

# ── Jena Fuseki ──────────────────────────────────────
COPY --from=fuseki_base  /jena-fuseki       /jena-fuseki
COPY --from=fuseki_base  /opt/java/java17   /opt/java/java17

# ── GraphDB ──────────────────────────────────────────
COPY --from=graphdb_base /opt/graphdb       /opt/graphdb
COPY --from=graphdb_base /opt/java/java11   /opt/java/java11

# ── Oxigraph ─────────────────────────────────────────
COPY --from=oxigraph_build /root/.cargo/bin/oxigraph /usr/local/bin/oxigraph

# ── Python evaluation framework ──────────────────────
COPY --from=python_base /starvers_eval         /starvers_eval
COPY --from=python_base /usr/local/lib/python3.11/site-packages \
                        /usr/local/lib/python3.11/site-packages

# ── Directory structure ───────────────────────────────
RUN mkdir -p \
    /starvers_eval/databases \
    /starvers_eval/output/{logs,measurements,result_sets,figures} \
    /starvers_eval/rawdata \
    /starvers_eval/queries/raw_queries/{beara/{low,high},bearb/{lookup,join},bearc/complex,orkg/complex} \
    /starvers_eval/scripts/{1_download,2_preprocess_data,3_construct_datasets,4_ingest,5_construct_queries,6_evaluate,7_visualize}

# ── System packages ───────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash coreutils procps grep sed curl bc wget iproute2 \
    locales liblzma5 liblzo2-2 zlib1g \
    libraptor2-0 libserd-0-0 libboost-iostreams-dev \
    ca-certificates libstdc++6 libgcc-s1 libatomic1 \ 
    clang libclang-dev unzip build-essential pkg-config libssl-dev \
    && sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

# ── Environment variables ─────────────────────────────
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8
ENV PYTHONPATH=/starvers_eval
ENV PATH=/usr/local/bin:$PATH
ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV NODE_PATH=/usr/local/lib/node_modules:/opt/comunica-feature-versioning/node_modules

ENV GDB_JAVA_OPTS='\
-Xms10g -Xmx70g \
-Dgraphdb.dist=/opt/graphdb/dist \
-Dgraphdb.home.work=/tmp/graphdb/work \
-Dgraphdb.workbench.importDirectory=/opt/graphdb/home/graphdb-import \
-Dgraphdb.workbench.cors.enable=true \
-Denable-context-index=true \
-Dentity-pool-implementation=transactional \
-Dhealth.max.query.time.seconds=30 \
-Dgraphdb.append.request.id.headers=true \
-Dreuse.vars.in.subselects=true'

ENV FUSEKI_HOME=/jena-fuseki
ENV JVM_ARGS='-Xms10g -Xmx80g'
ENV ADMIN_PASSWORD=starvers