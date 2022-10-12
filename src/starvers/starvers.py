from ._helper import template_path, versioning_timestamp_format, to_df, pprintAlgebra
from ._prefixes import versioning_prefixes, split_prefixes_query
from ._exceptions import RDFStarNotSupported, NoConnectionToRDFStore, NoVersioningMode, \
    WrongInputFormatException, ExpressionNotCoveredException

from urllib.error import URLError
from enum import Enum
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON
import pandas as pd
from datetime import datetime
import logging
import tzlocal
from datetime import datetime, timedelta, timezone

from rdflib.term import Variable, Identifier, URIRef
from rdflib.plugins.sparql.parserutils import CompValue
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
import rdflib.plugins.sparql.parser
from rdflib.paths import SequencePath, Path, NegatedPath, AlternativePath, InvPath, MulPath, ZeroOrOne, \
    ZeroOrMore, OneOrMore

class VersioningMode(Enum):
    Q_PERF = 1
    SAVE_MEM = 2


class TripleStoreEngine:
    """

    """

    class Credentials:

        def __init__(self, user_name: str, pw: str):
            self.user_name = user_name
            self.pw = pw

    def __init__(self, query_endpoint: str, update_endpoint: str, credentials: Credentials = None,
                 skip_connection_test=False):
        """
        During initialization a few queries are executed against the RDF* store to test connection but also whether
        the RDF* store in fact supports the 'star' extension. During the execution a side effect may occur and
        additional triples may be added by the RDF* store. These triples are pure meta data triples and reflect
        classes and properties (like rdf:type and rdfs:subPropertyOf) of RDF itself. This happens due to a new prefix,
        namely, vers: <https://github.com/GreenfishK/DataCitation/versioning/>' which is used in the write statements.
        Upon execution, this prefix gets embedded into the RDF class hierarchy by the RDF store, thus, new triples
        are written to the store.

        :param query_endpoint: URL for executing read/select statements on the RDF store. In GRAPHDB this URL can be
        looked up under "Setup --> Repositories --> Link icon"
        :param update_endpoint: URL for executing write statements on the RDF store. Its URL is an extension of
        query_endpoint: "query_endpoint/statements"
        :param credentials: The user name and password for the remote RDF store
        """

        self.credentials = credentials
        self._template_location = template_path("templates")

        self.sparql_get = SPARQLWrapper(query_endpoint)
        self.sparql_get.setHTTPAuth(DIGEST)
        self.sparql_get.setMethod(GET)
        self.sparql_get.setReturnFormat(JSON)

        self.sparql_get_with_post = SPARQLWrapper(query_endpoint)
        self.sparql_get_with_post.setHTTPAuth(DIGEST)
        self.sparql_get_with_post.setMethod(POST)
        self.sparql_get_with_post.setReturnFormat(JSON)

        self.sparql_post = SPARQLWrapper(update_endpoint)
        self.sparql_post.setHTTPAuth(DIGEST)
        self.sparql_post.setMethod(POST)

        if self.credentials is not None:
            self.sparql_post.setCredentials(credentials.user_name, credentials.pw)
            self.sparql_get.setCredentials(credentials.user_name, credentials.pw)

        if not skip_connection_test:
            # Test connection. Execute one read and one write statement
            try:
                self.sparql_get.setQuery(open(self._template_location +
                                              "/test_connection/test_connection_select.txt", "r").read())

                insert_statement = open(self._template_location +
                                        "/test_connection/test_connection_insert.txt", "r").read()
                self.sparql_post.setQuery(insert_statement)
                self.sparql_post.query()

                delete_statement = open(self._template_location +
                                        "/test_connection/test_connection_delete.txt", "r").read()
                self.sparql_post.setQuery(delete_statement)
                self.sparql_post.query()

            except URLError:
                raise NoConnectionToRDFStore("No connection to the RDF* store could be established. "
                                             "Check whether your RDF* store is running.")

            try:
                test_prefixes = versioning_prefixes("")
                template = open(self._template_location +
                                "/test_connection/test_connection_nested_select.txt", "r").read()
                select_statement = template.format(test_prefixes)
                self.sparql_get.setQuery(select_statement)
                self.sparql_get.query()

                template = open(self._template_location +
                                "/test_connection/test_connection_nested_insert.txt", "r").read()
                insert_statement = template.format(test_prefixes)
                self.sparql_post.setQuery(insert_statement)
                self.sparql_post.query()

                template = open(self._template_location +
                                "/test_connection/test_connection_nested_delete.txt", "r").read()
                delete_statement = template.format(test_prefixes)
                self.sparql_post.setQuery(delete_statement)
                self.sparql_post.query()

            except Exception:
                raise RDFStarNotSupported("Your RDF store might not support the 'star' extension. "
                                          "Make sure that it is a RDF* store.")

            logging.info("Connection to RDF query and update endpoints "
                         "{0} and {1} established".format(query_endpoint, update_endpoint))
        else:
            logging.info("Connection test has been skipped")


    def _delete_triples(self, triples: list, prefixes: dict = None):
        """
        Deletes the triples and its version annotations from the history. Should be used with care
        as it is most of times not intended to delete triples but to outdate them. This way they will
        still appear in the history and will not appear when querying more recent versions.

        :param triples: Triples in n3 syntax to be deleted
        :param prefixes: Prefixes used in triples.
        :return:
        """

        statement = open(self._template_location + "/_delete_triples.txt", "r").read()

        if prefixes:
            sparql_prefixes = versioning_prefixes(prefixes)
        else:
            sparql_prefixes = versioning_prefixes("")

        # Handling input format
        trpls = []
        if not isinstance(triples[0], list) and len(triples) == 3:
            triple = triples
            trpls.append(triple)
        else:
            trpls = triples

        for triple in trpls:
            if isinstance(triple, list) and len(triple) == 3:
                s = triple[0]
                p = triple[1]
                o = triple[2]

                delete_statement = statement.format(sparql_prefixes, s, p, o)
                self.sparql_post.setQuery(delete_statement)
                self.sparql_post.query()
                logging.info("Triple {0} successfully deleted: ".format(triple))
            else:
                e = "Please provide either a list of lists with three elements - subject, predicate and object or a " \
                    "single list with aforementioned three elements in n3 syntax. "
                logging.error(e)
                raise WrongInputFormatException(e)

    def _reset_all_versions(self):
        """
        Delete all triples with vers:valid_from and vers:valid_until as predicate. 
        Should not be used in a normal situation but rather if something went wrong the the timestamps need to be reset.

        :return:
        """

        template = open(self._template_location + "/_reset_all_versions.txt", "r").read()
        delete_statement = template.format(versioning_prefixes(""))
        self.sparql_post.setQuery(delete_statement)
        self.sparql_post.query()

        logging.info("All annotations have been removed.")


    def version_all_rows(self, initial_timestamp: datetime = None,
                         versioning_mode: VersioningMode = VersioningMode.Q_PERF):
        """
        Version all triples with an artificial end date. If the mode is Q_PERF then every triple is additionally
        annotated with a valid_from date where the date is the initial_timestamp provided by the caller.

        :param versioning_mode: The mode to use for versioning your data in the RDF store. The Q_PERF mode takes up
        more storage as for every triple in the RDF store two additional triples are added. In return, querying
        timestamped data is faster. The SAVE_MEM mode only adds one additional metadata triple per data triple
        to the RDF store. However, the queries are more time-consuming as additional filters are needed.
        Make sure to choose the mode the better suits your need as the mode gets set only once at the beginning.
        Every subsequent query that gets send to the RDF endpoint using query() will also operate in the chosen mode.
        :param initial_timestamp: Timestamp which also must include the timezone. Only relevant for Q_PERF mode.
        :return:
        """

        final_prefixes = versioning_prefixes("")
        versioning_mode_dir1 = self._template_location + "/init_versioning_modes"
        versioning_mode_dir2 = self._template_location + "/query_modes"

        if versioning_mode == VersioningMode.Q_PERF and initial_timestamp is not None:
            version_timestamp = versioning_timestamp_format(initial_timestamp)

            versioning_mode_template1 = open(versioning_mode_dir1 + "/version_all_rows_q_perf.txt", "r").read()
            versioning_mode_template2 = \
                open(versioning_mode_dir2 + "/versioning_query_extensions_q_perf.txt", "r").read()
            update_statement = versioning_mode_template1.format(final_prefixes, version_timestamp)
            message = "All rows have been annotated with start date {0} " \
                      "and an artificial end date".format(initial_timestamp)
        elif versioning_mode == VersioningMode.SAVE_MEM:
            versioning_mode_template1 = open(versioning_mode_dir1 + "/version_all_rows_save_mem.txt", "r").read()
            versioning_mode_template2 = \
                open(versioning_mode_dir2 + "/versioning_query_extensions_save_mem.txt", "r").read()
            update_statement = versioning_mode_template1.format(final_prefixes)
            message = "All rows have been annotated with an artificial end date."
        else:
            raise NoVersioningMode("Versioning mode is neither Q_PERF nor SAVE_MEM. Initial versioning will not be"
                                   "executed. Check also whether an initial timestamp was passed in case of Q_PERF.")

        with open(self._template_location + "/version_all_rows.txt", "w") as vers:
            vers.write(versioning_mode_template1)
        with open(self._template_location + "/versioning_query_extensions.txt", "w") as vers:
            vers.write(versioning_mode_template2)

        self.sparql_post.setQuery(update_statement)
        self.sparql_post.query()

        logging.info(message)


    def _timestamp_query(self, query, version_timestamp: datetime = None) -> str:
        """
        Binds a q_handler timestamp to the variable ?TimeOfExecution and wraps it around the query. Also extends
        the query with a code snippet that ensures that a snapshot of the data as of q_handler
        time gets returned when the query is executed. Optionally, but recommended, the order by clause
        is attached to the query to ensure a unique sort of the data.

        :param query:
        :param version_timestamp:
        :return: A query string extended with the given timestamp
        """
        logging.info("Creating timestamped query ...")
        prefixes, query = split_prefixes_query(query)
        query_vers = prefixes + "\n" + query

        if version_timestamp is None:
            current_datetime = datetime.now()
            timezone_delta = tzlocal.get_localzone().dst(current_datetime).seconds
            execution_datetime = datetime.now(timezone(timedelta(seconds=timezone_delta)))
            timestamp = versioning_timestamp_format(execution_datetime)
        else:
            timestamp = versioning_timestamp_format(version_timestamp)  # -> str

        query_tree = parser.parseQuery(query_vers)
        query_algebra = algebra.translateQuery(query_tree)

        bgp_triples = {}
        # TODO: set type of node to CompValue in function head
        def inject_versioning_extensions(node):
            if isinstance(node, CompValue):
                if node.name == "BGP":
                    bgp_id = "BGP_" + str(len(bgp_triples))
                    bgp_triples[bgp_id] = node.triples.copy()

                    dummy_block = [rdflib.term.Literal('__{0}dummy_subject__'.format(bgp_id)),
                    rdflib.term.Literal('__{0}dummy_predicate__'.format(bgp_id)),
                    rdflib.term.Literal('__{0}dummy_object__'.format(bgp_id))]
                    node.triples = []
                    node.triples.append(dummy_block)
                    
                elif node.name=="Builtin_NOTEXISTS" or node.name=="Builtin_EXISTS":
                    algebra.traverse(node.graph, visitPre=inject_versioning_extensions)

        def resolve_paths(node: CompValue):
            if isinstance(node, CompValue):
                if node.name == "BGP":
                    resolved_triples = []

                    def resolve(path: Path, subj, obj):
                        if isinstance(path, SequencePath):
                            for i, ref in enumerate(path.args, start=1):
                                if i == 1:
                                    s = subj
                                    p = ref
                                    o = Variable("?dummy{0}".format(str(i)))
                                elif i == len(path.args):
                                    s = Variable("?dummy{0}".format(len(path.args) - 1))
                                    p = ref
                                    o = obj
                                else:
                                    s = Variable("?dummy{0}".format(str(i - 1)))
                                    p = ref
                                    o = Variable("?dummy{0}".format(str(i)))
                                if isinstance(ref, URIRef):
                                    t = (s, p, o)
                                    resolved_triples.append(t)
                                    continue
                                if isinstance(ref, Path):
                                    resolve(p, s, o)
                                else:
                                    raise ExpressionNotCoveredException("Node inside Path is neither Path nor URIRef but: "
                                                                        "{0}. This case has not been covered yet. "
                                                                        "Path will not be resolved.".format(type(ref)))

                        if isinstance(path, NegatedPath):
                            raise ExpressionNotCoveredException("NegatedPath has not be covered yet. Path will not be resolved")
                        if isinstance(path, AlternativePath):
                            raise ExpressionNotCoveredException("AlternativePath has not be covered yet. "
                                                                "Path will not be resolved. Instead of alternative paths "
                                                                "try using the following expression: "
                                                                "{ <triple statements> } "
                                                                "UNION "
                                                                "{ <triple statements> }")
                        if isinstance(path, InvPath):
                            if isinstance(path.arg, URIRef):
                                t = (obj, path.arg, subj)
                                resolved_triples.append(t)
                                return
                            else:
                                raise ExpressionNotCoveredException("An argument for inverted paths other than URIRef "
                                                                    "was given. This case is not implemented yet.")  
                        
                        if isinstance(path, MulPath):
                            if path.mod == "?":
                                raise ExpressionNotCoveredException("ZeroOrOne path has not be covered yet. "
                                                                    "Path will not be resolved")
                            if path.mod == "*": 
                                raise ExpressionNotCoveredException("ZeroOrMore path has not be covered yet. "
                                                                    "Path will not be resolved")
                            if path.mod == "+":
                                raise ExpressionNotCoveredException("OneOrMore path has not be covered yet. "
                                                                    "Path will not be resolved")

                    for k, triple in enumerate(node.triples):
                        if isinstance(triple[0], Identifier) and isinstance(triple[2], Identifier):
                            if isinstance(triple[1], Path):
                                resolve(triple[1], triple[0], triple[2])
                            else:
                                if isinstance(triple[1], Identifier):
                                    resolved_triples.append(triple)
                                else:
                                    raise ExpressionNotCoveredException("Predicate is neither Path nor Identifier but: {0}. "
                                                                        "This case has not been covered yet.".format(triple[1]))
                        else:
                            raise ExpressionNotCoveredException("Subject and/or object are not identifiers but: {0} and {1}."
                                                                " This is not implemented yet.".format(triple[0], triple[2]))

                    node.triples.clear()
                    node.triples.extend(resolved_triples)
                    node.triples = algebra.reorderTriples(node.triples)

                elif node.name=="Builtin_NOTEXISTS" or node.name=="Builtin_EXISTS":
                    algebra.traverse(node.graph, visitPre=resolve_paths)
                
        try:
            logging.info("Resolving SPARQL paths to normal triple statements ...")
            algebra.traverse(query_algebra.algebra, visitPre=resolve_paths)
            logging.info("Injecting versioning extensions into query ...")
            algebra.traverse(query_algebra.algebra, visitPre=inject_versioning_extensions)
        except ExpressionNotCoveredException as e:
            err = "Query will not be timestamped because of following error: {0}".format(e)
            raise ExpressionNotCoveredException(err)
        
        # Create the SPARQL representation from the query algebra tree.
        query_vers_out = algebra.translateAlgebra(query_algebra) 

        # Replace each block of triples (labeled as dummy block) 
        # with their corresponding block of timestamped triple statements.
        triple_stmts_cnt = 0
        for bgp_identifier, triples in bgp_triples.items():
            ver_block_template = \
                open(template_path("templates/versioning_query_extensions.txt"), "r").read()

            ver_block = ""
            for i, triple in enumerate(triples):
                triple_stmts_cnt = triple_stmts_cnt + 1
                logging.debug(triple_stmts_cnt)
                templ = ver_block_template
                triple_n3 = triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3()
                ver_block += templ.format(triple_n3,
                                          "?valid_from_{0}".format(str(triple_stmts_cnt)),
                                          "?valid_until_{0}".format(str(triple_stmts_cnt)),
                                          bgp_identifier)

            # 
            dummy_triple = rdflib.term.Literal('__{0}dummy_subject__'.format(bgp_identifier)).n3() + " "\
                           + rdflib.term.Literal('__{0}dummy_predicate__'.format(bgp_identifier)).n3() + " "\
                           + rdflib.term.Literal('__{0}dummy_object__'.format(bgp_identifier)).n3() + "."
            ver_block += 'bind("{0}"^^xsd:dateTime as ?ts{1})'.format(timestamp, bgp_identifier)
            query_vers_out = query_vers_out.replace(dummy_triple, ver_block)

        query_vers_out = versioning_prefixes("") + "\n" + query_vers_out
        
        return query_vers_out, timestamp


    def query(self, select_statement, timestamp: datetime = None, yn_timestamp_query: bool = True) -> pd.DataFrame:
        """
        Executes the SPARQL select statement and returns a result set. If :timestamp is provided the result set
        will be a snapshot of the data as of :timestamp. Otherwise, the most recent version of the data will be returned.

        :param select_statement: A SPARQL query that is a select statement.
        :param timestamp: The version/snapshot timestamp for which a snapshot of the data as of :timestamp should be retrieved.
        :param yn_timestamp_query: If true, the select statement will be transformed into a timestamped query. 
        Otherwise, the select statement is executed as it is against the RDF store. 
        Set this flag to 'False' and leave :timestamp blank if :select_statement is a timestamped query already.

        :return: a pandas dataframe of the RDF result set.
        """

        if yn_timestamp_query:
            timestamped_query, version_timestamp = self._timestamp_query(query=select_statement, version_timestamp=timestamp)

            logging.info("Timestamped query with timestamp {0} being executed:"
                         " \n {1}".format(version_timestamp, timestamped_query))
            self.sparql_get_with_post.setQuery(timestamped_query)
        else:
            logging.info("Query being executed: \n {0}".format(select_statement))
            self.sparql_get_with_post.setQuery(select_statement)
        
        # The query sometimes gets recognized as LOAD even though it is a SELECT statement. this results into
        # a failed execution as we are using an get endpoint which is not allowed with LOAD
        self.sparql_get_with_post.queryType = 'SELECT'
        logging.info("Retrieving results ...")
        result = self.sparql_get_with_post.query()
        logging.info("Converting results ... ")
        df = to_df(result)

        return df


    def insert(self, triples: list, prefixes: dict = None):
        """
        Inserts a list of triples (must be in n3 syntax!) into the RDF* store and two additional (nested) triples
        for each new triple labeling the newly inserted triple with a valid_from and valid_until date.

        :param triples: A list of list of triples in n3 syntax!
        E.g. 
        [['<http://example.com/Obama>', '<http://example.com/president_of>' ,'<http://example.com/UnitedStates'],
        ['<http://example.com/Hamilton>', '<http://example.com/occupation>', '<http://example.com/Formel1Driver']]
        :param prefixes: Prefixes that are used within :param triples
        :return:
        """

        statement = open(self._template_location + "/insert_triples.txt", "r").read()

        if prefixes:
            sparql_prefixes = versioning_prefixes(prefixes)
        else:
            sparql_prefixes = versioning_prefixes("")

        if len(triples) == 0:
            raise Exception ("List is empty. No triples will be inserted.")

        insert_block = ""
        for triple in triples:
            if isinstance(triple, list) and len(triple) == 3:
                s = triple[0]
                p = triple[1]
                o = triple[2]

                insert_block = insert_block + "(" + s + " " + p + " " + o + " )\n"

                logging.info("Triple {0} successfully inserted: ".format(triple))
            else:
                e = "The triple is either not of type list or the list does not have the length 3."
                logging.error(e)
                raise WrongInputFormatException(e)
        insert_statement = statement.format(sparql_prefixes, insert_block)
        self.sparql_post.setQuery(insert_statement)
        self.sparql_post.query()


    def update(self, triples: dict, prefixes: dict = None):
        """
        Updates all triples' objects that are provided in :triples as key values with the corresponding
        values from the same dictionary. the triples and the new value must be in n3 syntax.
        Only the most recent triples (those that are annotated with
        <<s p o >> vers:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime) will be updated.

        :param triples: A dictionary with triples as key values and strings as values. All triple elements
        and corresponding new values must be provided as strings and may also contain SPARQL prefixes. E.g. foaf:name
        :param prefixes: Prefixes that are used within :param triples
        """

        template = open(self._template_location + "/update_triples.txt", "r").read()
        for i, (triple, new_value) in enumerate(triples.items()):
            if isinstance(triple, tuple) and isinstance(new_value, str):
                sparql_prefixes = versioning_prefixes(prefixes)
                update_statement = template.format(sparql_prefixes, triple[0], triple[1], triple[2], new_value)
                self.sparql_post.setQuery(update_statement)
                result = self.sparql_post.query()
                logging.info("{0} rows updated".format(result))
            else:
                raise WrongInputFormatException("Wrong input format. The update statement will not be executed. "
                                                "Please provide :triples.key() as a tuple (str, str, str) "
                                                "and :triples.value() as a string.")


    def outdate(self, triples: list, prefixes: dict = None):
        """
        Outdates all triples' that are provided in :triples by annotating them with a timestamp returned by
        SPARQL's NOW() function. the triples provided must be in n3 syntax.

        :param triples: A list of tuples where each tuple is a triple (s, p, o) -> (str, str, str).
        All triple elements must be provided  as strings and may also contain SPARQL prefixes. E.g. foaf:name
        :param prefixes: Prefixes that are used within :param triples
        """

        template = open(self._template_location + "/outdate_triples.txt", "r").read()
        for triple in triples:
            if isinstance(triple, tuple):
                sparql_prefixes = versioning_prefixes(prefixes)
                update_statement = template.format(sparql_prefixes, triple[0], triple[1], triple[2])
                self.sparql_post.setQuery(update_statement)
                result = self.sparql_post.query()
                logging.info("{0} rows outdated".format(result))
            else:
                raise WrongInputFormatException("Wrong input format. The update statement will not be executed. "
                                                "Please provide :triples as a list of tuples (str, str, str). ")

    