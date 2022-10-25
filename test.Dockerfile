FROM stain/jena-fuseki:4.0.0 AS jena
FROM python:3.8.15-slim
COPY --from=jena /fuseki /fuseki