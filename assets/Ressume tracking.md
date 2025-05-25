# How to ressume a tracking service after the container has been deleted?
Let's assume that the data is in the GraphDB repository 'air_quality_ontology_v2'. Follow the steps below:
- Change the repository name in the POST statement to a temporary name, e.g. 'air_quality_ontology_v3'
```
{
  "name": "Air Quality Data for a City Ontology",
  "repository_name": "air_quality_ontology_v3",
  "rdf_dataset_url": "https://vocab.linkeddata.es/datosabiertos/def/medio-ambiente/calidad-aire/ontology.nt",
  "polling_interval": 7200,
  "delta_type": "SPARQL"
}
```
- Send the POST statement to localhost:80/management
- The PollingTask service will start tracking the corresponding ontology under the name 'air_quality_ontology_v3'
- Login to the postgres container via a shell: `psql -d starvers_db -U user -W`
When prompted, write 'passwpord'. Then update the ontology name to the previous repository where 
your data resides, i.e. to 'air_quality_ontology_v2'.
```
update dataset set repository_name = 'schema_org_ontology_v2'
where repository_name = 'schema_org_ontology_v3';
```

- Restart the starversserver service with `docker-compose up starversserver`.