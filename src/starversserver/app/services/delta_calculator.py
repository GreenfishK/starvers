"""
delta_calculator.py

Two implementations for computing the delta between consecutive RDF snapshots:

  IterativeDeltaCalculator  — loads both the stored triples and the new dump into
                               Python sets, then computes set differences.

  SparqlDeltaCalculator     — loads the new snapshot into a temporary GraphDB graph
                               and runs SPARQL CONSTRUCT queries to find insertions
                               and deletions.

Both implement the same interface: calculate_delta() → (insertions, deletions).
"""

import datetime
from abc import ABC, abstractmethod
from typing import List, Tuple

from app.utils.starvers.starvers import TripleStoreEngine
from app.LoggingConfig import get_logger
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.HelperService import to_list
from app.persistance.graphdb.GraphDatabaseUtils import (
    get_construct_all_versioned_template,
    get_delta_query_deletions_template,
    get_delta_query_insertions_template,
    get_drop_graph_template,
)


class DeltaCalculator(ABC):
    """Common interface for delta calculation strategies."""

    def __init__(self, sparql_engine: TripleStoreEngine, tracking_task: TrackingTaskDto, repository_name: str):
        self.sparql_engine    = sparql_engine
        self.tracking_task    = tracking_task
        self.repository_name  = repository_name
        self.LOG = get_logger(__name__)

    @abstractmethod
    def calculate_delta(self, version_timestamp: datetime.datetime, *args, **kwargs) -> Tuple[List[str], List[str]]:
        """Return (insertions, deletions) as lists of N-Triple strings."""

    @abstractmethod
    def clean_up(self):
        """Release any temporary resources (e.g. temporary graphs in the triple store)."""


# ---------------------------------------------------------------------------
# Iterative (set-difference) implementation
# ---------------------------------------------------------------------------

class IterativeDeltaCalculator(DeltaCalculator):
    """
    Computes the delta by comparing two N-Triple sets in memory:
      - The current snapshot file (new dump).
      - The triples currently stored in the versioned triple store.
    """

    def calculate_delta(
        self, version_timestamp: datetime.datetime, processed_path: str = ""
    ) -> Tuple[List[str], List[str]]:

        # Load new snapshot from disk into a set of N-Triple strings
        self.LOG.info(f"[{self.repository_name}] Iterative: loading new snapshot from {processed_path}.")
        with open(processed_path, "r", encoding="utf-8") as f:
            new_triples = set(to_list(f.read()))

        # Query the current version from the triple store via CONSTRUCT
        self.LOG.info(f"[{self.repository_name}] Iterative: querying current version from triple store.")
        self.sparql_engine.sparql_get_with_post.setReturnFormat("n3")
        self.sparql_engine.sparql_get_with_post.addCustomHttpHeader("Accept", "application/n-triples")
        self.sparql_engine.sparql_get_with_post.setQuery(get_construct_all_versioned_template(version_timestamp))
        stored_n3_str = self.sparql_engine.sparql_get_with_post.query().convert().decode("utf-8")

        # Restore default headers
        self.sparql_engine.sparql_get_with_post.setReturnFormat("json")
        self.sparql_engine.sparql_get_with_post.clearCustomHttpHeader("Accept")

        stored_triples = set(to_list(stored_n3_str))

        # Set difference gives us the delta
        insertions = list(new_triples    - stored_triples)
        deletions  = list(stored_triples - new_triples)

        self.LOG.info(f"[{self.repository_name}] Iterative delta: +{len(insertions)} / -{len(deletions)}")
        return insertions, deletions

    def clean_up(self):
        pass  # No temporary resources to clean up


# ---------------------------------------------------------------------------
# SPARQL (graph-diff) implementation
# ---------------------------------------------------------------------------

class SparqlDeltaCalculator(DeltaCalculator):
    """
    Computes the delta by loading the new snapshot into a temporary named graph
    and running SPARQL CONSTRUCT queries to find differences against the current version.
    """

    def calculate_delta(self, version_timestamp: datetime.datetime) -> Tuple[List[str], List[str]]:
        self.sparql_engine.sparql_get_with_post.setReturnFormat("n3")
        self.sparql_engine.sparql_get_with_post.addCustomHttpHeader("Accept", "application/n-triples")

        # Query insertions (triples in the new graph but not in the versioned store)
        self.LOG.info(f"[{self.repository_name}] SPARQL: querying delta insertions between the last version at {version_timestamp} and the new graph.")
        self.sparql_engine.sparql_get_with_post.setQuery(
            get_delta_query_insertions_template(version_timestamp, self.tracking_task.name_temp())
        )
        insertions = to_list(self.sparql_engine.sparql_get_with_post.query().convert().decode("utf-8"))

        # Query deletions (triples in the versioned store but not in the new graph)
        self.LOG.info(f"[{self.repository_name}] SPARQL: querying delta deletions between the last version at {version_timestamp} and the new graph.")
        self.sparql_engine.sparql_get_with_post.setQuery(
            get_delta_query_deletions_template(version_timestamp, self.tracking_task.name_temp())
        )
        deletions = to_list(self.sparql_engine.sparql_get_with_post.query().convert().decode("utf-8"))

        # Restore default headers
        self.sparql_engine.sparql_get_with_post.setReturnFormat("json")
        self.sparql_engine.sparql_get_with_post.clearCustomHttpHeader("Accept")

        self.LOG.info(f"[{self.repository_name}] SPARQL delta: +{len(insertions)} / -{len(deletions)}")
        return insertions, deletions

    def clean_up(self):
        """Drop the temporary named graph from the triple store."""
        temp_graph = self.tracking_task.name_temp()
        self.LOG.info(f"[{self.repository_name}] SPARQL: dropping temp graph '{temp_graph}'.")
        self.sparql_engine.sparql_post.setQuery(get_drop_graph_template(temp_graph))
        self.sparql_engine.sparql_post.query()
        self.LOG.info(f"[{self.repository_name}] SPARQL: temp graph '{temp_graph}' dropped.")
