# Starvers
A repository for a timestamp-based versioning API for RDF data. The module starvers leverages sparql-star's and rdf-star's nested triples paradigm to automatically embedds timestamps into every SPARQL 1.1 update statement or query. This API enables to programatically create temporal RDF knowledge graphs or ontologies whereas the version timestamps are instrinsic porperties of these RDF datasets. Thus, no other versioning tools like Git are required and the datasets are portable to any triplestore that supports multilevel nesting with RDF-star and SPARQL-star. 
Conceptually, the API transforms every read or write statement into a timestamped statement. This way it is possible to query arbitrary snapshots of the RDF data by sending the query + a timestamp via the API interface. In the following we will guide you through the installation process and provide give examples of how the offered functions should used to operate on an RDF dataset.

# Installation
TODO: Explain how to install the python package

# Example usage
For every operation we need to create a constructor and setup a connection to a triple store that supports multilevel nesting with RDF-star and SPARQL-star, such as GraphDB. 

```
from starvers.starvers import TripleStoreEngine

get_endpoint = "http://your-machine:7200/repositories/your-repository"
post_endpoint = "http://your-machine:7200/repositories/your-repository/statements"
engine = TripleStoreEngine(get_endpoint, post_endpoint)

```

## Version all rows - initialize dataset
First, we need to initialize our dataset and wrap every triple with a valid\_from and a valid\_until timestamp.

```
engine.version_all_rows()
```


## Query actual or historical data
To query actual data we just need to pass the query as a string to the query function.

```
query = """
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?a (sum(?pageLength) as ?sumPageLength)
WHERE {
    ?a <http://dbpedia.org/ontology/wikiPageLength> ?pageLength .
    filter(?a = <http://dbpedia.org/resource/2015_ATP_Challenger_Tour> || ?a = <http://dbpedia.org/resource/2015_Pacific_hurricane_season>)

} group by ?a 
having (sum(?pageLength) > 5000000)
"""

# For the latest snapshot
engine.query(query)

# For a snapshot at a specific point in time
snapshot_timestamp = datetime(2020, 9, 1, 12, 11, 21, 941000,  timezone(timedelta(hours=2))
engine.query(query, snapshot_timestamp)
```

## Insert new triples
To insert new triples we first need to prepare a list of triples and then pass them to the insert function. The triples must already be in n3 syntax, i.e. in case of an IRI, include the pointy brackets < > in the string.

```
new_triples = [['<http://example.com/Obama>', '<http://example.com/president_of>' ,'<http://example.com/UnitedStates'],
        ['<http://example.com/Hamilton>', '<http://example.com/occupation>', '<http://example.com/Formel1Driver']]
engine.insert(new_triples)
```

## Update triples

## Delete (Outdate) triples