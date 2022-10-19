FROM ontotext/graphdb:9.11.2-se as graphdb
ENV GDB_JAVA_OPTS='\
-Xmx2g -Xms2g \
-Dgraphdb.home=/opt/graphdb/home \
-Dgraphdb.workbench.importDirectory=/opt/graphdb/home/graphdb-import \
-Dgraphdb.workbench.cors.enable=true \
-Denable-context-index=true \
-Dentity-pool-implementation=transactional \
-Dhealth.max.query.time.seconds=60 \
-Dgraphdb.append.request.id.headers=true \
-Dreuse.vars.in.subselects=true'
COPY configs/graphdb.license /opt/graphdb/home/conf/
