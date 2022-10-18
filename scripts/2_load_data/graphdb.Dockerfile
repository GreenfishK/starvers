FROM ontotext/graphdb:9.11.2-se as graphdb

ARG configFile
COPY configs/$configFile /opt/graphdb/dist/conf/
COPY configs/graphdb.license /opt/graphdb/dist/conf/
