FROM python:3.8.15-slim
COPY --from=stain/jena-fuseki:4.0.0 /fuseki /fuseki