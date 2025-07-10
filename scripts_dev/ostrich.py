import SPARQLWrapper
import requests

sparql_endpoint = "http://start_ostrich_endpoint:42564/sparql"

# Check whether this endpoint is available
def is_endpoint_available(url):
    try:
        response = requests.head(url, timeout=5)
        return response.status_code == 200
    except Exception as e:
        print(f"Endpoint check failed: {e}")
        return False

if not is_endpoint_available(sparql_endpoint):
    print(f"SPARQL endpoint {sparql_endpoint} is not available.")
    exit(1)

def execute_sparql_query(query):
    url = sparql_endpoint
    headers = {
        "Content-Type": "application/sparql-query"
    }
    try:
        response = requests.post(url, data=query, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
query = """
SELECT * WHERE {
	GRAPH <version:0> {
		?s ?p ?o .
	}
} LIMIT 20
"""

print("Executing SPARQL query...")
results = execute_sparql_query(query)
if results:
    print("Parsing results...")
    for result in results["results"]["bindings"]:
        print(result)
else:
    print("No results found or an error occurred.")
