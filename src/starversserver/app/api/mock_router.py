"""
mock_router.py

Provides a self-updating mock RDF dataset (a small DBpedia snippet about Austria)
that changes every 2 seconds. Useful for testing and evaluating the polling pipeline
without needing a live external data source.

The mock graph is also exposed via an embedded SPARQL endpoint.
"""

import time
from datetime import timedelta

from fastapi import APIRouter, Response
from rdflib import ConjunctiveGraph
from rdflib_endpoint import SparqlRouter
from timeloop import Timeloop

tag = "mock"

tl = Timeloop()
g = ConjunctiveGraph()

router = APIRouter(prefix="/mock", tags=[tag])

tag_metadata = {
    "name": tag,
    "description": (
        "Mock endpoint that returns a modified DBpedia snippet about Austria. "
        "The population value updates every 2 seconds to simulate fast-changing data."
    ),
}

# Embedded SPARQL endpoint backed by the in-memory mock graph
sparql_router = SparqlRouter(
    graph=g,
    path="/mock/sparql",
    title="SPARQL endpoint for mock graph",
    description="A SPARQL endpoint to serve mock data for testing and evaluating polling tasks.",
    version="0.1.0",
)

# ---------------------------------------------------------------------------
# RDF template — population is substituted with the current nanosecond timestamp
# ---------------------------------------------------------------------------

_AUSTRIA_RDF_TEMPLATE = """<http://de.dbpedia.org/resource/Österreich> <http://dbpedia.org/ontology/areaTotal> "1.12E8"^^<http://www.w3.org/2001/XMLSchema#double> . 
<http://de.dbpedia.org/resource/Österreich> <http://dbpedia.org/ontology/topLevelDomain> <http://de.dbpedia.org/resource/.at> . 
<http://de.dbpedia.org/resource/Österreich> <http://dbpedia.org/ontology/populationTotal> "{:population}"^^<http://www.w3.org/2001/XMLSchema#nonNegativeInteger> . 
<http://de.dbpedia.org/resource/Österreich> <http://dbpedia.org/ontology/abstract> "Österreich ( [ˈøːstɐʁaɪ̯ç]; amtlich Republik Österreich) ist ein mitteleuropäischer Binnenstaat mit rund 8,9 Millionen Einwohnern. Die angrenzenden Staaten sind Deutschland und Tschechien im Norden, die Slowakei und Ungarn im Osten, Slowenien und Italien im Süden sowie die Schweiz und Liechtenstein im Westen. Österreich ist ein demokratischer und föderaler Bundesstaat, im Besonderen eine semipräsidentielle Republik. Seine großteils aus den historischen Kronländern hervorgegangenen neun Bundesländer sind das Burgenland, Kärnten, Niederösterreich, Oberösterreich, Salzburg, die Steiermark, Tirol, Vorarlberg und Wien. Das Bundesland Wien ist zugleich Bundeshauptstadt und auch einwohnerstärkste Stadt des Landes. Weitere Bevölkerungszentren sind Graz, Linz, Salzburg und Innsbruck. Das Land wird von der Böhmischen Masse und der Thaya im Norden, den Karawanken und dem Steirischen Hügelland im Süden, der Pannonischen Tiefebene im Osten sowie dem Rhein und dem Bodensee im Westen begrenzt. Mehr als 62 Prozent seiner Staatsfläche werden von alpinem Hochgebirge gebildet. Der österreichische Staat wird deshalb auch als Alpenrepublik bezeichnet. Die Bezeichnung Österreich ist in ihrer althochdeutschen Form Ostarrichi erstmals aus dem Jahr 996 überliefert. Daneben war ab dem frühen Mittelalter die lateinische Bezeichnung Austria in Verwendung. Ursprünglich eine Grenzmark des Stammesherzogtums Baiern, wurde Österreich 1156 zu einem im Heiligen Römischen Reich eigenständigen Herzogtum erhoben. Nach dem Aussterben des Geschlechts der Babenberger 1246 setzte sich das Haus Habsburg im Kampf um die Herrschaft in Österreich durch. Das als Österreich bezeichnete Gebiet umfasste später die gesamte Habsburgermonarchie sowie in der Folge das 1804 konstituierte Kaisertum Österreich und die österreichische Reichshälfte der 1867 errichteten Doppelmonarchie Österreich-Ungarn. Die heutige Republik entstand ab 1918, nach dem für Österreich-Ungarn verlorenen Ersten Weltkrieg, aus den zunächst Deutschösterreich genannten deutschsprachigen Teilen der Monarchie. Mit dem Vertrag von Saint-Germain wurden die Staatsgrenze und der Name Republik Österreich festgelegt. Damit einher ging der Verlust Südtirols. Die Erste Republik war von innenpolitischen Spannungen geprägt, die in einen Bürgerkrieg und die Ständestaatsdiktatur mündeten. Durch den sogenannten „Anschluss“ stand das Land ab 1938 unter nationalsozialistischer Herrschaft. Nach der Niederlage des Deutschen Reiches im Zweiten Weltkrieg wieder ein eigenständiger Staat, erklärte Österreich am Ende der alliierten Besatzung 1955 seine immerwährende Neutralität und trat den Vereinten Nationen bei. Österreich ist seit 1956 Mitglied im Europarat, Gründungsmitglied der 1961 errichteten Organisation für wirtschaftliche Zusammenarbeit und Entwicklung (OECD) und seit 1995 ein Mitgliedsstaat der Europäischen Union."@de .""" 


# ---------------------------------------------------------------------------
# Background job: refresh the mock graph every 2 seconds
# ---------------------------------------------------------------------------

@tl.job(interval=timedelta(seconds=2))
def _refresh_mock_graph(graph: ConjunctiveGraph = g):
    """Clear the graph and reload it with an updated population value."""
    graph -= graph.triples((None, None, None))
    graph.parse(data=_AUSTRIA_RDF_TEMPLATE.replace("{:population}", str(time.time_ns())))
    graph.commit()


# ---------------------------------------------------------------------------
# HTTP endpoint: serve the mock graph as N-Triples
# ---------------------------------------------------------------------------

@router.get("/n-triples.nt")
async def get_mock_rdf_austria():
    """Return the current mock graph serialised as N-Triples."""
    return Response(content=g.serialize(format="nt"), media_type="application/n-triples")
