# Information about the process
The TDB database files are created during runtime via the Java TDBLoader API and do not need to be manually loaded via CLI. 
The raw RDF(-star) datasets are taken from the location which is provided as parameter in the "run docker" script mentioned above. The TDB datasets are then mounted onto Jena's fuseki server which is created during runtime. This enables the queries to be executed via http requests. \
To load the raw dataset files into an embedded GraphDB repository we use a Sail configuration file together with a couple of java constructors and method calls. \

Note that we are not evaluating IC or CB policies as for [Data Citation](https://rd-alliance.org/system/files/documents/RDA-DC-Recommendations_151020.pdf), which is our main motivation for this project, timestamped-based versioning is recommended. This can, nevertheless, still be done with our script by passing the corresponding parameters. However, one needs to load the TDB datasets manually first and direct java invocation will be used for these policies, as in the original BEAR and OSTRICH framework.

