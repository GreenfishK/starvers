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
First, we need to initialize our dataset and wrap every triple with a valid\_from and a valid\_until timestamp. Consider following example RDF dataset:

| Subject      | Predicate | Object |
| ----------- | ----------- | ----------- |
| <http://example.com/Obama> | <http://example.com/occupation> |<http://example.com/President> |
| <http://example.com/Hamilton> | <http://example.com/occupation> | <http://example.com/Formel1Driver> |

Now we can choose whether we want to timestamp the data with the execution timestamp or with a custom one. For this example, we chose a custom timestamp in order to make the example reproducible. By executing ...
```
initial_timestamp = datetime(2022, 10, 12, 14, 43, 21, 941000, timezone(timedelta(hours=2)))
engine.version_all_rows(initial_timestamp)
# alternatively: engine.version_all_rows()
```
... our dataset turns into:

| Subject      | Predicate | Object |
| ----------- | ----------- | ----------- |
| << << <http://example.com/Obama> <http://example.com/occupation> <http://example.com/President> >> https://github.com/GreenfishK/DataCitation/versioning/valid_from "2022-10-12T14:43:21.941000+02:00"^^xsd:dateTime >> | https://github.com/GreenfishK/DataCitation/versioning/valid_until | "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime |
| << << <http://example.com/Hamilton> <http://example.com/occupation> <http://example.com/Formel1Driver> >> https://github.com/GreenfishK/DataCitation/versioning/valid_from "2022-10-12T14:43:21.941000+02:00"^^xsd:dateTime >> | https://github.com/GreenfishK/DataCitation/versioning/valid_until | "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime |

## Insert new triples
To insert new triples we first need to prepare a list of triples and then pass them to the insert function. The triples must already be in n3 syntax, i.e. in case of an IRI, include the pointy brackets < > in the string.

```
new_triples = [['<http://example.com/Brad_Pitt>', '<http://example.com/occupation>' ,'<http://example.com/Limo_Driver>'],
        ['<http://example.com/Frank_Sinatra>', '<http://example.com/occupation>', '<http://example.com/Singer>']]
engine.insert(new_triples)
```

## Update triples
To update triples we need to provide two lists of triples - one with the triples to be updated and one with the new values. Essentially, these are two nx3 matrices where one gets overriden by the other. If a value should not be updated None should be simply passed to the new matrix on the desired position. In the following example we are updating the subject position in the first triple and the object position in the second triple.

```
engine.update(
old_triples=[['<http://example.com/Obama>', '<http://example.com/occupation>' ,'<http://example.com/President>'],
             ['<http://example.com/Brad_Pitt>', '<http://example.com/occupation>', '<http://example.com/Limo_Driver>']],
new_triples=[['<http://example.com/Donald_Trump>', None, None],
             [None, None, '<http://example.com/Actor>']])

```

## Delete (Outdate) triples
To outdate triples we need to provide a list of valid triples which should be deleted. The valid_until timestamp of any matched triple will be replaced by the current system timestamp of python's datetime.now() function.
```
engine.outdate([['<http://example.com/Donald_Trump>', '<http://example.com/occupation>' ,'<http://example.com/President>']])
```


## Query actual or historical data
To query actual data we just need to pass the query as a string ... 

```
query = """
PREFIX vers: <https://github.com/GreenfishK/DataCitation/versioning/>

SELECT ?person ?occupation {
    ?person <http://example.com/occupation> ?occupation .
}
"""
```
... to the query function. 
```
actual_snapshot = engine.query(query)
print(actual_snapshot)
```
Result set:
| person       | occupation |
| ----------- |  ----------- |
| <http://example.com/Hamilton> | <http://example.com/Formel1Driver> |
| <http://example.com/Brad_Pitt> | <http://example.com/Actor> |
| <http://example.com/Frank_Sinatra> | <http://example.com/Singer> |

To query historical data we additionally need to pass a timestamp. Here we chose the initial timestamp when we versioned our dataset for the first time ([see above](#version-all-rows---initialize-dataset))
```
snapshot_timestamp = initial_timestamp
historical_snapshot = engine.query(query, snapshot_timestamp)
print(historical_snapshot)
```
Result set:
| person       | occupation |
| ----------- |  ----------- |
| <http://example.com/Obama> | <http://example.com/President> |
| <http://example.com/Hamilton> | <http://example.com/Formel1Driver> |
