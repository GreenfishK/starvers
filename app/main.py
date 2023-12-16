from fastapi import FastAPI
from starvers.starvers import TripleStoreEngine

app = FastAPI();

get_endpoint = "http://localhost:7200/repositories/Test"
post_endpoint = "http://localhost:7200/repositories/Test/statements"
engine = TripleStoreEngine(get_endpoint, post_endpoint)

new_triples = ['<http://example.com/Brad_Pitt> <http://example.com/occupation> <http://example.com/Limo_Driver> .',
               '<http://example.com/Frank_Sinatra> <http://example.com/occupation> <http://example.com/Singer> .']
engine.insert(new_triples)

query = """
PREFIX vers: <https://github.com/GreenfishK/DataCitation/versioning/>

SELECT ?person ?occupation {
    ?person <http://example.com/occupation> ?occupation .
}
"""

actual_snapshot = engine.query(query)
print(actual_snapshot)