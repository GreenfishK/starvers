# OSTRICH + HDT auf Ubuntu 20.04
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    build-essential git curl ca-certificates \
    cmake ninja-build \
    autoconf automake libtool pkg-config \
    zlib1g-dev liblzma-dev liblzo2-dev \
    libraptor2-dev \
    libserd-dev \
    libkyotocabinet-dev \
    libboost-iostreams-dev \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

# --- HDT (hdt-cpp) mit Autotools bauen ---
RUN git clone --recursive https://github.com/rdfhdt/hdt-cpp.git && \
    cd hdt-cpp && \
    ./autogen.sh && \
    ./configure && \
    make -j"$(nproc)" && \
    make install && \
    ldconfig

# --- OSTRICH klonen (und auf 1.0.0 pinnen) ---
RUN git clone --recursive https://github.com/rdfostrich/ostrich.git && \
    cd /opt/ostrich && \
    git checkout 1.0.0 && \
    git submodule update --init --recursive

# Hunter nur für Boost deaktivieren -> System-Boost verwenden
RUN sed -i 's/^[[:space:]]*hunter_add_package(Boost[^)]*)/# DISABLED: hunter_add_package(Boost)\nfind_package(Boost REQUIRED COMPONENTS iostreams)/' /opt/ostrich/CMakeLists.txt


# Patch: rvalue std::string -> lvalue Variable für std::regex_match
WORKDIR /opt/ostrich
RUN sed -i 's/if(std::regex_match(std::string(ent->d_name), base_match, r)) {/const std::string _name(ent->d_name);\n            if(std::regex_match(_name, base_match, r)) {/' src/main/cpp/snapshot/snapshot_manager.cc && \
    sed -i 's/if(std::regex_match(std::string(ent->d_name), base_match, r)) {/const std::string _name(ent->d_name);\n            if(std::regex_match(_name, base_match, r)) {/' src/main/cpp/patch/patch_tree_manager.cc


# --- OSTRICH bauen (system libs nutzen) ---
WORKDIR /opt/ostrich
RUN mkdir -p build && cd build && \
    cmake -G Ninja .. \
      -DCMAKE_BUILD_TYPE=Release \
      -DHUNTER_ROOT=/root/.hunter \
      -DCMAKE_PREFIX_PATH="/usr/local;/usr" && \
    ninja ostrich-insert ostrich-evaluate && \
    ninja ostrich-query-version || true && \
    ninja ostrich-query-delta-materialized || true && \
    ninja ostrich-query-version-materialized || true && \
    ninja ostrich-statistics || true

ENV PATH="/opt/ostrich/build:${PATH}"
WORKDIR /data

# Standard-Kommando anzeigen
CMD ["bash"]
