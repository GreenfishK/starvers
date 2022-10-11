GraphDB 9.3 was used for this test. Below are the endpoints for a local repository. 
The dataset in this repository is an RDF-star variant of the BEAR-B hourly dataset (https://doi.org/10.5281/zenodo.5877503 -> alldata.TB_star_hierarchical.ttl).

TODO: Use a docker container for GraphDB : https://github.com/Ontotext-AD/graphdb-docker
TODO: 
* Load two datasets - the last snapshot extracted from the RDF-star variant of BEAR-B-hourly in RDF and the BEAR-B hourly RDF-star dataset. 
* Execute the SPARQL test queries against the last snapshot and their SPARQL-star variants against the RDF-star dataset. Assert that the results are equal.

The SPARQL construct statement used to construct/extract the last snapshot/independent copy of the BEAR-B hourly dataset. This dataset was used to compare the results of the test queries with the results of the timestamped test queries executed against the RDF-star'd BEAR-B dataset. Whenever the tests are run the execution timestamp gets embedded into the test queries (=timestamped test queries). This timestamp is always greater than the greatest timestamp of the RDF-star'd BEAR-B dataset. Hence, the latest snapshot is always queried.
```
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vers: <https://github.com/GreenfishK/DataCitation/versioning/>

construct { ?s ?p ?o} where { 
	<< <<?s ?p ?o >> vers:valid_from ?valid_from_1 >> vers:valid_until ?valid_until_1.
    filter(?valid_from_1 <= ?tsBGP_1 && ?tsBGP_1 < ?valid_until_1)
    bind("2022-10-10T09:54:57.161373+01:00"^^xsd:dateTime as ?tsBGP_1)
} 
```