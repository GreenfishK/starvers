FROM stain/jena-fuseki:4.0.0 as install_jena
WORKDIR /
RUN ls

FROM ontotext/graphdb:9.11.2-se
WORKDIR /
COPY --from=install_jena /fuseki/. /fuseki