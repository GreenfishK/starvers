from ._helper import versioning_timestamp_format, to_df
from ._prefixes import add_versioning_prefixes, split_prefixes_query
from .exceptions import RDFStarNotSupported, NoConnectionToRDFStore, \
WrongInputFormatException, ExpressionNotCoveredException
    
from urllib.error import URLError
from SPARQLWrapper import Wrapper, SPARQLWrapper, POST, DIGEST, GET, JSON
import pandas as pd
import os
from datetime import datetime
import logging
import time
from zoneinfo import ZoneInfo
from typing import Optional
from datetime import datetime
from typing import Union
import rdflib
from rdflib.term import Variable, Identifier, URIRef
from rdflib.plugins.sparql.parserutils import CompValue
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
from rdflib.paths import SequencePath, Path, NegatedPath, AlternativePath, InvPath, MulPath #, ZeroOrOne, ZeroOrMore, OneOrMore

logger = logging.getLogger(__name__)

def timestamp_query(query: str, version_timestamp: Optional[datetime] = None) -> Union[str, str]:
    """
    Binds a q_handler timestamp to the variable ?TimeOfExecution and wraps it around the query. Also extends
    the query with a code snippet that ensures that a snapshot of the data as of q_handler
    time gets returned when the query is executed. Optionally, but recommended, the order by clause
    is attached to the query to ensure a unique sort of the data.

    :param query:
    :param version_timestamp:
    :return: A query string extended with the given timestamp
    """
    logger.info("Creating timestamped query ...")
    prefixes, query = split_prefixes_query(query)
    query_vers = prefixes + "\n" + query

    if version_timestamp is None:
        tz_name = time.tzname[time.localtime().tm_isdst]  # standard or DST name
        local_tz = ZoneInfo(tz_name)
        execution_datetime = datetime.now(local_tz)
        timestamp = versioning_timestamp_format(execution_datetime)
    else:
        timestamp = versioning_timestamp_format(version_timestamp)  

    query_tree = parser.parseQuery(query_vers)
    query_algebra = algebra.translateQuery(query_tree)

    bgp_triples = {} 
    def inject_versioning_extensions(node: object):
        if not isinstance(node, CompValue):
            return  

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

    def resolve_paths(node: object):
        if not isinstance(node, CompValue):
            return  
        
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
                    # TODO resolve path
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
        logger.info("Resolving SPARQL paths to normal triple statements ...")
        algebra.traverse(query_algebra.algebra, visitPre=resolve_paths)
        logger.info("Injecting versioning extensions into query ...")
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
        
        template_path = os.path.join(os.path.dirname(__file__), "templates/versioning_query_extensions.txt")
        ver_block_template = \
            open(template_path, "r").read()

        ver_block = ""
        for i, triple in enumerate(triples):
            triple_stmts_cnt = triple_stmts_cnt + 1
            logger.debug(triple_stmts_cnt)
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

    query_vers_out = add_versioning_prefixes("") + "\n" + query_vers_out
    
    return query_vers_out, timestamp


class TripleStoreEngine:
    """

    """

    class Credentials:

        def __init__(self, user_name: str, pw: str):
            self.user_name = user_name
            self.pw = pw

    def __init__(self, query_endpoint: str, update_endpoint: str, credentials: Optional[Credentials] = None,
                 skip_connection_test: bool=False):
        """
        During initialization a few queries are executed against the RDF-star store to test connection but also whether
        the RDF-star store in fact supports the 'star' extension. During the execution a side effect may occur and
        additional triples may be added by the RDF-star store. These triples are pure meta data triples and reflect
        classes and properties (like rdf:type and rdfs:subPropertyOf) of RDF itself. This happens due to a new prefix,
        namely, vers: <https://github.com/GreenfishK/DataCitation/versioning/>' which is used in the write statements.
        Upon execution, this prefix gets embedded into the RDF class hierarchy by the RDF-star store, thus, new triples
        are written to the store.

        :param query_endpoint: URL for executing read/select statements on the RDF-star store. In GRAPHDB this URL can be
        looked up under "Setup --> Repositories --> Link icon"
        :param update_endpoint: URL for executing write statements on the RDF-star store. Its URL is an extension of
        query_endpoint: "query_endpoint/statements"
        :param credentials: The user name and password for the remote RDF-star store
        """

        self.credentials = credentials
        self._template_location = os.path.join(os.path.dirname(__file__), "templates")

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

        self.timestamped_query = None

        if credentials:
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
                raise NoConnectionToRDFStore("No connection to the RDF-star store could be established. "
                                             "Check whether your RDF-star store is running.")

            try:
                test_prefixes = add_versioning_prefixes("")
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
                raise RDFStarNotSupported("Your RDF-star store might not support the 'star' extension. "
                                          "Make sure that it is a RDF* store.")

            logger.info("Connection to RDF-star query and update endpoints "
                         "{0} and {1} established".format(query_endpoint, update_endpoint))
        else:
            logger.info("Connection test has been skipped")


    def _delete_triples(self, triples: Union[list[str], list[list[str]]], prefixes: Optional[dict[str, str]] = None):
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
            sparql_prefixes = add_versioning_prefixes(prefixes)
        else:
            sparql_prefixes = add_versioning_prefixes("")

        # single triple [s, p, o] 
        if len(triples) == 3 and all(isinstance(x, str) for x in triples):
            trpls = [triples]
        # list of triples [[s, p, o], ...]
        elif all(isinstance(t, list) for t in triples):
            trpls = triples
        else:
            raise WrongInputFormatException(
                "Provide either a single triple [s,p,o] or a list of triples [[s,p,o], ...]."
            )

        for triple in trpls:
            if isinstance(triple, list) and len(triple) == 3:
                s = triple[0]
                p = triple[1]
                o = triple[2]

                delete_statement = statement.format(sparql_prefixes, s, p, o)
                self.sparql_post.setQuery(delete_statement)
                self.sparql_post.query()
                logger.info("Triple {0} successfully deleted: ".format(triple))
            else:
                e = "Please provide either a list of lists with three elements - subject, predicate and object or a " \
                    "single list with aforementioned three elements in n3 syntax. "
                logger.error(e)
                raise WrongInputFormatException(e)

    def _reset_all_versions(self):
        """
        Delete all triples with vers:valid_from and vers:valid_until as predicate. 
        Should not be used in a normal situation but rather if something went wrong the the timestamps need to be reset.

        :return:
        """

        template = open(self._template_location + "/_reset_all_versions.txt", "r").read()
        delete_statement = template.format(add_versioning_prefixes(""))
        self.sparql_post.setQuery(delete_statement)
        self.sparql_post.query()

        logger.info("All annotations have been removed.")


    def version_all_triples(self, initial_timestamp: Optional[datetime] = None):
        """
        Versions all triples by wrapping every triple in the dataset with the execution timestamp as valid_from date 
        and an end date that lies far in the future as valid_until date. 

        :param initial_timestamp: A timestamp which should be used as the valid_from timestamp. If this parameter is None,
        the current system timestamp will be used.
        :return:
        """

        final_prefixes = add_versioning_prefixes("")

        if initial_timestamp:
            version_timestamp = versioning_timestamp_format(initial_timestamp)
        else:
            version_timestamp = versioning_timestamp_format(datetime.now().astimezone())

        temp = open(self._template_location + "/version_all_triples.txt", "r").read()
        update_statement = temp.format(final_prefixes, version_timestamp)

        self.sparql_post.setQuery(update_statement)
        self.sparql_post.query()
        logger.info("All rows have been annotated with start date {0} " \
                    "and an artificial end date 9999-12-31T00:00:00.000+02:00".format(version_timestamp))


    def query(self, select_statement: str, timestamp: Optional[datetime] = None, yn_timestamp_query: bool = True, as_df: bool = True) -> Union[pd.DataFrame, Wrapper.QueryResult]:
        """
        Executes the SPARQL select statement and returns a result set. If :timestamp is provided the result set
        will be a snapshot of the data as of :timestamp. Otherwise, the most recent version of the data will be returned.

        :param select_statement: A SPARQL query that is a select statement.
        :param timestamp: The version/snapshot timestamp for which a snapshot of the data as of :timestamp should be retrieved.
        :param yn_timestamp_query: If true, the select statement will be transformed into a timestamped query. 
        Otherwise, the select statement is executed as it is against the RDF-star store. 
        Set this flag to 'False' and leave :timestamp blank if :select_statement is a timestamped query already.
        :param as_df: If true, the result set will be converted into a pandas dataframe.
        """

        if yn_timestamp_query:
            timestamped_query, version_timestamp = timestamp_query(query=select_statement, version_timestamp=timestamp)

            logger.info("Timestamped query with timestamp {0} being executed:"
                         " \n {1}".format(version_timestamp, timestamped_query))
            self.sparql_get_with_post.setQuery(timestamped_query)
            self.timestamped_query = timestamped_query
        else:
            logger.info("Query being executed: \n {0}".format(select_statement))
            self.sparql_get_with_post.setQuery(select_statement)
        
        #self.sparql_get_with_post.queryType = 'SELECT'
        logger.info("Retrieving results ...")
        result = self.sparql_get_with_post.query()

        logger.info(f"The result has the return type {result._get_responseFormat()}.")

        if not as_df:
            logger.info("Returning raw result ...")
            return result
        else:
            logger.info("Converting results to pandas dataframe ...")
            df = to_df(result)
            return df


    def retrieve_snapshot(self, timestamp: Optional[datetime] = None) -> str:
        """
        Executes a predefined SPARQL construct query and returns a result set as string in n3 syntax. If :timestamp is provided 
        the result set will be a snapshot of the data as of :timestamp. Otherwise, the most recent version of the data 
        will be returned.

        :param timestamp: The version timestamp for which a snapshot of the data as of :timestamp should be retrieved.
        """

        snapshot_construct_query = open(self._template_location + "/timestamped_construct_query.txt", "r").read()
        if timestamp:
            snapshot_construct_query = snapshot_construct_query.format('"' + versioning_timestamp_format(timestamp) + '"')
        else:
            snapshot_construct_query = snapshot_construct_query.format("NOW()")

        logger.info("Snapshot construct query being executed: \n {1}".format(snapshot_construct_query))
        self.sparql_get_with_post.setReturnFormat('n3') 
        self.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')
        self.sparql_get_with_post.setQuery(snapshot_construct_query)

        logger.info("Retrieving results ...")
        result = self.sparql_get_with_post.query().convert().decode("utf-8")

        # return to default behaviour
        self.sparql_get_with_post.setReturnFormat('json') 
        self.sparql_get_with_post.clearCustomHttpHeader('Accept')

        return result


    def insert(self, triples: Union[list[str], str], prefixes: Optional[dict[str, str]] = None, timestamp: Optional[datetime] = None, chunk_size: int = 1000):
        """
        Inserts a list of nested triples into the RDF-star store by wrapping the provided triples with a valid_from (NOW()) and 
        "artificial" valid_until timestamp using the RDF-star paradigm. Each inserted triple has the following form 
        where the strings in curly brackets are replaced during execution time: 
            << {s} {p} {o} >> vers:valid_from "{valid_from}"^^xsd:datetime >> vers:valid_until "9999-31-12T00:00:00.000+02:00"^^xsd:dateTime .
        The triples must be provided in n3 syntax. 
        Blank nodes are not supported currently and will yield a SPARQL exception. 
        E.g.: 
        ['<http://example.com/Obama> <http://example.com/president_of> <http://example.com/UnitedStates .,
        <http://example.com/Hamilton> <http://example.com/occupation> <http://example.com/Formel1Driver .']

        or the whole insert block for the VALUES clause:
        "(<http://example.com/Obama> <http://example.com/president_of> <http://example.com/UnitedStates) 
        (<http://example.com/Hamilton>' '<http://example.com/occupation>' '<http://example.com/Formel1Driver)"

        :param triples: A list of list of triples in n3 syntax (including the dot) or a string in the SPARQL syntax for the VALUES block.
        :param prefixes: Prefixes that are used within :param triples.
        :param timestamp: If a timestamp is given, the inserted triples will be annotated with this timestamp.
        :param chunk_size: The maximum number of triples that are inserted during each iteration. If the dataset is greater than :chunk_size 
        the SPARQL updates are split into chunks where one chunk has maximum :chunk_size triples. It can be useful to experiment 
        with this parameter and find the optimal chunk size for the target triple store.
        :return:
        """

        if len(triples) == 0:
            logger.info("List is empty. No triples will be outdated.")
            return

        if prefixes:
            sparql_prefixes = add_versioning_prefixes(prefixes)
        else:
            sparql_prefixes = add_versioning_prefixes("")

        logger.info("Creating insert statement.")
        statement = open(self._template_location + "/insert_triples.txt", "r").read()

        if isinstance(triples, list):
            logger.info("Creating insert statement: Build insert block.")
            for i, line in enumerate(triples):
                triples[i] = line[:-2]
            insert_list = list(map(list, zip(['('] * len(triples), triples, [')'] * len(triples))))
            insert_block = list(map(' '.join, insert_list))
        else:
            logger.info("Creating insert statement: Build insert block.")
            insert_block = triples.splitlines()

        logger.info("Inserting triples as chunks of {0} triples.".format(chunk_size))
        for i in range(0, len(insert_block), chunk_size):
            insert_chunk = "\n".join(insert_block[i:min(i+chunk_size, len(insert_block))])
            if timestamp:
                version_timestamp = versioning_timestamp_format(timestamp)
                insert_statement = statement.format(sparql_prefixes, insert_chunk, '"' + version_timestamp + '"')
            else:
                insert_statement = statement.format(sparql_prefixes, insert_chunk, "NOW()")
            self.sparql_post.setQuery(insert_statement)
            self.sparql_post.query()
        logger.info("Triples inserted.")


    def update(self, old_triples: list[list[str]], new_triples: list[list[str]], prefixes: Optional[dict[str,str]] = None, chunk_size: int = 1000):
        """
        Updates a list of triples by another list of triples. Both lists need to have the same dimensions. The first list 
        should contain triples in n3 syntax that are also present in the triple store and currently valid. Each triple in the 
        second list should contain a new value in n3 syntax for the corresponding element in the first list and None if this element should 
        not be updated. 
        E.g.:
        old_triples = 
        [['<http://example.com/Obama>', '<http://example.com/president_of>' ,'<http://example.com/UnitedStates'],
        ['<http://example.com/Hamilton>', '<http://example.com/occupation>', '<http://example.com/Formel1Driver']]

        new_triples =
        [[None, None,'<http://example.com/Canada'],
        ['<http://example.com/Lewis_Hamilton>', None, None]]


        :param old_triples: A list of valid triples in n3 syntax that should be updated.
        :param new_triples: A list of new values for the list :old_triples. Values which should not be updated must be None.
        :param prefixes: Prefixes that are used within :old_triples and :new_triples.
        """

        if len(old_triples) == 0:
            logger.info("List is empty. No triples will be outdated.")
            return

        if len(old_triples) != len(new_triples):
            raise WrongInputFormatException("Both lists old_triples and new_triples must have the same dimensions.")

        if prefixes:
            sparql_prefixes = add_versioning_prefixes(prefixes)
        else:
            sparql_prefixes = add_versioning_prefixes("")

        logger.info("Create update statement")
        template = open(self._template_location + "/update_triples.txt", "r").read()

        update_block: list[str] = []
        for old_triple, new_triple in zip(old_triples, new_triples):
            if not (len(old_triple) == 3 and len(new_triple) == 3):
                raise WrongInputFormatException("The old or new triple's length is not 3.")
            newS, newP, newO = ["UNDEF" if v is None else v for v in new_triple]
            update_block.append(f"({old_triple[0]} {old_triple[1]} {old_triple[2]} {newS} {newP} {newO})")
        
        logger.info("Updating triples as chunks of {0} triples.".format(chunk_size))
        for i in range(0, len(update_block), chunk_size):
            update_chunk = "\n".join(update_block[i:min(i+chunk_size, len(update_block))])
            update_statement = template.format(sparql_prefixes, update_chunk)
            self.sparql_post.setQuery(update_statement)
            self.sparql_post.query()
        logger.info("Triples updated.")
        

    def outdate(self, triples: Union[list[str], str], prefixes: Optional[dict[str,str]] = None, timestamp: Optional[datetime] = None, chunk_size: int = 1000):
        """
        Outdates a list of triples. The provided triples are matched against the latest snapshot of the RDF-star dataset 
        and their valid_until timestamps get replaced by the query execution timestamp (SPARQL NOW() function) or the given :timestamp.
        The triples that are match have the following form where {valid_until} gets replaced during execution time:
            << s p o >> vers:valid_from "valid_from"^^xsd:datetime >> vers:valid_until "{valid_until}"^^xsd:dateTime .
        The provided triples in :triples must be in n3 syntax. Blank nodes are not supported currently and will yield a SPARQL exception. 
        E.g.: 
        ['<http://example.com/Obama> <http://example.com/president_of> <http://example.com/UnitedStates .,
        <http://example.com/Hamilton> <http://example.com/occupation> <http://example.com/Formel1Driver .']

        or the whole insert block for the VALUES clause:
        "(<http://example.com/Obama> <http://example.com/president_of> <http://example.com/UnitedStates) 
        (<http://example.com/Hamilton>' '<http://example.com/occupation>' '<http://example.com/Formel1Driver)"

        :param triples: A list of list of triples in n3 syntax (including the dot) or a string in the SPARQL syntax for the VALUES block.
        :param prefixes: Prefixes that are used within :param triples.
        :param timestamp: If a timestamp is given, the outdated triples will be annotated with this timestamp.
        :param chunk_size: The maximum number of triples that are outdated during each iteration. If the dataset is greater than :chunk_size 
        the SPARQL updates are split into chunks where one chunk has maximum :chunk_size triples. It can be useful to experiment 
        with this parameter and find the optimal chunk size for the target triple store.
        :return:
        """

        if len(triples) == 0:
            logger.info("List is empty. No triples will be outdated.")
            return

        if prefixes:
            sparql_prefixes = add_versioning_prefixes(prefixes)
        else:
            sparql_prefixes = add_versioning_prefixes("")

        logger.info("Creating outdate statement.")
        statement = open(self._template_location + "/outdate_triples.txt", "r").read()

        if isinstance(triples, list):
            logger.info("Creating outdate statement:Build outdate block.")
            for i, line in enumerate(triples):
                triples[i] = line[:-2]
            outdate_list = list(map(list, zip(['('] * len(triples), triples, [')'] * len(triples))))
            outdate_block = list(map(' '.join, outdate_list))
        else:
            logger.info("Creating outdate statement: Build outdate block.")
            outdate_block = triples.splitlines()

        
        logger.info("Outdating triples as chunks of {0} triples.".format(chunk_size))
        for i in range(0, len(outdate_block), chunk_size):
            outdate_chunk = "\n".join(outdate_block[i:min(i+chunk_size, len(outdate_block))])
            if timestamp:
                version_timestamp = versioning_timestamp_format(timestamp)
                outdate_statement = statement.format(sparql_prefixes, outdate_chunk, '"' + version_timestamp + '"')
            else:
                outdate_statement = statement.format(sparql_prefixes, outdate_chunk, "NOW()")
            self.sparql_post.setQuery(outdate_statement)
            self.sparql_post.query()
        logger.info("Triples outdated.")

    
