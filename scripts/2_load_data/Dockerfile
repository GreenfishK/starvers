FROM ontotext/graphdb:9.11.2-se
ENV GDB_JAVA_OPTS='\
-Xmx2g -Xms2g \
-Dgraphdb.home.data=/opt/graphdb/home/data \
-Dgraphdb.workbench.importDirectory=/opt/graphdb/home/graphdb-import \
-Dgraphdb.workbench.cors.enable=true \
-Denable-context-index=true \
-Dentity-pool-implementation=transactional \
-Dhealth.max.query.time.seconds=60 \
-Dgraphdb.append.request.id.headers=true \
-Dreuse.vars.in.subselects=true '

ARG configFile
COPY configs/$configFile /opt/graphdb/dist/conf/
COPY configs/graphdb.license /opt/graphdb/dist/conf/

ENTRYPOINT []