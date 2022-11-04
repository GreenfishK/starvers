import re
from rdflib import Graph

g = Graph()
g.parse("input.nt")

for s,p,o in g:
    print(s,p,o)
