from cmath import isnan
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
from rdflib.plugins.sparql.parserutils import CompValue

query = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

select ?p ?o {
	<http://dbpedia.org/resource/Donald_Trump> ?p ?o .
    filter not exists {
        <http://dbpedia.org/resource/Donald_Trump> rdf:type ?o
    }
}
"""

query_tree = parser.parseQuery(query)
query_algebra = algebra.translateQuery(query_tree)
algebra.pprintAlgebra(query_algebra)
print("\n\n")

def check_not_exists(node: CompValue):
    if isinstance(node, CompValue):
        #if node.name.endswith("NOTEXISTS"):
        #    print(node, end="\n\n")
        #    print(node.graph)
        if node.name == "BGP":
            print(node)
        if node.name=="Builtin_NOTEXISTS":
            algebra.traverse(node.graph, visitPre=check_not_exists)


node_algebra = algebra.traverse(query_algebra.algebra, visitPre=check_not_exists)
