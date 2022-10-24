# Clone starvers from Github
FROM alpine/git:2.36.3 as base
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
COPY /data/queries /starvers_eval/queries 
COPY scripts_dev/1_get_and_prepare_data/build_tb_rdf_star_datasets.py /starvers_eval/scripts/1_get_and_prepare_data
COPY scripts_dev/1_get_and_prepare_data/data_corrections.py /starvers_eval/scripts/1_get_and_prepare_data
COPY scripts_dev/1_get_and_prepare_data/download_data.sh /starvers_eval/scripts/1_get_and_prepare_data

# Install starvers (build) and modules from requirements.txt
FROM python:3.8.15-slim as install_dependencies
COPY --from=base /starvers /starvers
COPY --from=base /starvers_eval /starvers_eval
RUN python3 -m venv /starvers_eval/python_venv
RUN . /starvers_eval/python_venv/bin/activate
WORKDIR /starvers
RUN /starvers_eval/python_venv/bin/python3 -m pip install .
COPY scripts_dev/requirements.txt /starvers_eval
WORKDIR /starvers_eval
RUN /starvers_eval/python_venv/bin/python3 -m pip install -r requirements.txt

FROM ontotext/graphdb:9.11.2-se as install_graphdb
COPY --from=install_dependencies /starvers_eval /starvers_eval
COPY scripts_dev/2_load_data/configs/graphdb.license /opt/graphdb/home/conf/
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

FROM stain/jena-fuseki:4.0.0
COPY --from=install_graphdb /starvers_eval /starvers_eval
COPY --from=install_graphdb /opt/graphdb /opt/graphdb



# RUN RUN is an image build step, the state of the container after a RUN command will be committed to the container image. 
# A Dockerfile can have many RUN steps that layer on top of one another to build the image. 
# CMD is the command the container executes by default when you launch the built image. CMD is similar to entrypoint
# ENTRYPOINT parameters cannot be overriden ny command-line parameters in the `docker run` while with CMD this is possible.
# The last FROM command in the dockerfile creates the actual final image.