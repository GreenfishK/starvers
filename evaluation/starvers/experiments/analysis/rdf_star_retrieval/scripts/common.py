"""
common.py – shared helpers for the RDF-star retrieval/indexing analysis.

Both the GraphDB and Jena analysis scripts read the same set of "retrieval
scenarios": a label -- the SPARQL(pattern) that the evaluator actually issues for
a given model and dataset -- and a corresponding explain/plan query per store.

A "scenario" is a dict:
    {
      'id':            str,   # e.g. 'decorator_label' / 'reification_label'
      'model':         str,   # 'tb_sr_rs' (decorator) or 'tb_sr_re' (reification)
      'graphdb_query': str,   # SPARQL sent to GraphDB (wrapped in FROM onto:explain)
      'jena_query':    str,   # SPARQL sent to Jena (--explain shows the plan)
    }

The predicate is configurable so the same scenario can run on any dataset.
"""

from pathlib import Path

VERS = "https://github.com/GreenfishK/DataCitation/versioning/"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


def scenario_triple(model: str) -> str:
    """Return the inner content << s p o >> triple pattern for a model."""
    if model == "tb_sr_rs":  # decorator / double nesting
        return "<< <<?s <{p}> ?o>> <{v}valid_from> ?vf >> <{v}valid_until> ?vu ."
    if model == "tb_sr_re":  # reification
        return "?b <{r}reifies> <<?s <{p}> ?o>> ; <{v}valid_from> ?vf ; <{v}valid_until> ?vu ."
    raise ValueError(f"unknown model: {model}")


def build_scenarios(predicate: str) -> list[dict]:
    p_fmt = dict(v=VERS, r=RDF, p=predicate)
    shared_q = (
        f"PREFIX vers: <{VERS}>\n"
        f"PREFIX rdf: <{RDF}>\n"
        f"SELECT ?s ?o {{ {scenario_triple('tb_sr_rs').format(**p_fmt)} }}"
    )
    return [
        {
            "id": "decorator_label",
            "model": "tb_sr_rs",
            "graphdb_query": (
                f"PREFIX onto: <http://www.ontotext.com/>\n"
                f"{shared_q.replace('SELECT ?s ?o', 'SELECT ?s ?o FROM onto:explain')}"
            ),
            "jena_query": shared_q,
        },
        {
            "id": "reification_label",
            "model": "tb_sr_re",
            "graphdb_query": (
                f"PREFIX onto: <http://www.ontotext.com/>\n"
                f"PREFIX rdf: <{RDF}>\n"
                f"SELECT ?s ?o FROM onto:explain"
                f" {{ {scenario_triple('tb_sr_re').format(**p_fmt)} }}"
            ),
            "jena_query": (
                f"PREFIX vers: <{VERS}>\n"
                f"PREFIX rdf: <{RDF}>\n"
                f"SELECT ?s ?o {{ {scenario_triple('tb_sr_re').format(**p_fmt)} }}"
            ),
        },
    ]


def pick_predicate(dataset: str) -> str:
    """A predicate that exists in every dataset (rdfs:label)."""
    return "http://www.w3.org/2000/01/rdf-schema#label"
