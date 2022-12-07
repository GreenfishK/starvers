from ._helper import template_path, versioning_timestamp_format, to_df
from ._prefixes import versioning_prefixes, split_prefixes_query
from ._exceptions import RDFStarNotSupported, NoConnectionToRDFStore, NoVersioningMode, \
    WrongInputFormatException, ExpressionNotCoveredException
import re
from urllib.error import URLError
from enum import Enum
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON
import pandas as pd
from datetime import datetime
import logging
import tzlocal
from datetime import datetime, timedelta, timezone
from typing import Union
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


def timestamp_query(query, version_timestamp: datetime = None) -> Union[str, str]:
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
                raise NoConnectionToRDFStore("No connection to the RDF-star store could be established. "
                                             "Check whether your RDF-star store is running.")

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
                raise RDFStarNotSupported("Your RDF-star store might not support the 'star' extension. "
                                          "Make sure that it is a RDF* store.")

            logging.info("Connection to RDF-star query and update endpoints "
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


    def version_all_rows(self, initial_timestamp: datetime = None):
        """
        Versions all triples by wrapping every triple in the dataset with the execution timestamp as valid_from date 
        and an end date that lies far in the future as valid_until date. 

        :param initial_timestamp: A timestamp which should be used as the valid_from timestamp. If this parameter is None,
        the current system timestamp will be used.
        :return:
        """

        final_prefixes = versioning_prefixes("")

        if initial_timestamp is not None:
            version_timestamp = versioning_timestamp_format(initial_timestamp)
        else:
            LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
            system_timestamp = datetime.now(tz=LOCAL_TIMEZONE)
            version_timestamp = versioning_timestamp_format(system_timestamp)

        temp = open(self._template_location + "/version_all_rows.txt", "r").read()
        update_statement = temp.format(final_prefixes, version_timestamp)

        self.sparql_post.setQuery(update_statement)
        self.sparql_post.query()
        logging.info("All rows have been annotated with start date {0} " \
                    "and an artificial end date 9999-12-31T00:00:00.000+02:00".format(version_timestamp))


    def query(self, select_statement, timestamp: datetime = None, yn_timestamp_query: bool = True) -> pd.DataFrame:
        """
        Executes the SPARQL select statement and returns a result set. If :timestamp is provided the result set
        will be a snapshot of the data as of :timestamp. Otherwise, the most recent version of the data will be returned.

        :param select_statement: A SPARQL query that is a select statement.
        :param timestamp: The version/snapshot timestamp for which a snapshot of the data as of :timestamp should be retrieved.
        :param yn_timestamp_query: If true, the select statement will be transformed into a timestamped query. 
        Otherwise, the select statement is executed as it is against the RDF-star store. 
        Set this flag to 'False' and leave :timestamp blank if :select_statement is a timestamped query already.

        :return: a pandas dataframe of the RDF result set.
        """

        if yn_timestamp_query:
            timestamped_query, version_timestamp = timestamp_query(query=select_statement, version_timestamp=timestamp)

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


    def insert(self, triples: list or str, prefixes: dict = None, timestamp: datetime = None):
        """
        Inserts a list of nested triples into the RDF-star store by wrapping the provided triples with a valid_from and 
        valid_until timestamp using the RDF-star paradigm. The triples must be provided in n3 syntax, 
        i.e. IRIs must be surrounded with pointy brackets < >.
        E.g.: 
        ['<http://example.com/Obama> <http://example.com/president_of> <http://example.com/UnitedStates .,
        <http://example.com/Hamilton> <http://example.com/occupation> <http://example.com/Formel1Driver .']

        or the whole insert block for the VALUES clause:
        "(<http://example.com/Obama> <http://example.com/president_of> <http://example.com/UnitedStates) 
        (<http://example.com/Hamilton>' '<http://example.com/occupation>' '<http://example.com/Formel1Driver)"

        :param triples: A list of list of triples in n3 syntax.
        :param prefixes: Prefixes that are used within :param triples.
        :param timestamp: If a timestamp is given, the inserted triples will be annotated with this timestamp.
        :return:
        """

        if len(triples) == 0:
            logging.info("List is empty. No triples will be outdated.")
            return

        if prefixes:
            sparql_prefixes = versioning_prefixes(prefixes)
        else:
            sparql_prefixes = versioning_prefixes("")

        logging.info("Creating insert statement.")
        statement = open(self._template_location + "/insert_triples.txt", "r").read()

        if isinstance(triples, list):
            logging.info("Creating insert statement: Build insert block.")
            for i, line in enumerate(triples):
                triples[i] = line[:-2]
            insert_list = list(map(list, zip(['('] * len(triples), triples, [')'] * len(triples))))
            insert_block = list(map(' '.join, insert_list))
        elif isinstance(triples, str):
            logging.info("Creating insert statement: Build insert block.")
            insert_block = triples.splitlines()
        else:
            raise Exception("Type of triples must be either list or string. See doc of this function.")

        logging.info("Inserting triples as batches of 1000 triples.")
        for i in range(0, len(insert_block), 1000):
            insert_batch = "\n".join(insert_block[i:min(i+1000, len(insert_block))])
            # Surround blank nodes in the subject position with pointy brackets
            insert_batch = re.sub(r'(?<=^\(\s)_:([a-zA-Z0-9]+)', r'<_:\1>', insert_batch)
            # Surround blank nodes in the object position with pointy brackets
            insert_batch = re.sub(r'_:([a-zA-Z0-9]+)\s*(?=\)(\s|$))', r'<_:\1>', insert_batch)
            logging.info(insert_batch)

            if timestamp:
                version_timestamp = versioning_timestamp_format(timestamp)
                insert_statement = statement.format(sparql_prefixes, insert_batch, '"' + version_timestamp + '"')
            else:
                insert_statement = statement.format(sparql_prefixes, insert_batch, "NOW()")
            self.sparql_post.setQuery(insert_statement)
            self.sparql_post.query()
        logging.info("Triples inserted.")


    def update(self, old_triples: list, new_triples: list, prefixes: dict = None):
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

        if len(old_triple) == 0:
            logging.info("List is empty. No triples will be outdated.")
            return

        if len(old_triples) != len(new_triples):
            raise WrongInputFormatException("Both lists old_triples and new_triples must have the same dimensions.")

        if prefixes:
            sparql_prefixes = versioning_prefixes(prefixes)
        else:
            sparql_prefixes = versioning_prefixes("")

        template = open(self._template_location + "/update_triples.txt", "r").read()
        update_block = ""
        for i, old_triple in enumerate(old_triples):
            new_triple = new_triples[i]
            if isinstance(old_triple, list) and isinstance(new_triple, list) \
            and len(old_triple) == 3 and len(new_triple) == 3:
                newS = new_triple[0] if new_triple[0] != None else "UNDEF"
                newP = new_triple[1] if new_triple[1] != None else "UNDEF"
                newO = new_triple[2] if new_triple[2] != None else "UNDEF"

                update_block = update_block + "({0} {1} {2} {3} {4} {5})\n".format(old_triple[0],old_triple[1],old_triple[2],
                newS, newP, newO)
            else:
                raise WrongInputFormatException("The triple is either not a list or its length is not 3.")
        update_statement = template.format(sparql_prefixes, update_block)
        self.sparql_post.setQuery(update_statement)
        self.sparql_post.query()
        logging.info("Triples updated.")



    def outdate(self, triples: list or str, prefixes: dict = None, timestamp: datetime = None):
        """
        Outdates a list of triples. The provided triples are matched against the latest snapshot of the RDF-star dataset 
        and their valid_until timestamps get replaced by the query execution timestamp (SPARQL NOW() function).
        The provided triples in :triples must be in n3 syntax, i.e. IRIs must be surrounded with pointy brackets < >. 
        E.g.: 
        ['<http://example.com/Obama> <http://example.com/president_of> <http://example.com/UnitedStates .,
        <http://example.com/Hamilton> <http://example.com/occupation> <http://example.com/Formel1Driver .']

        or the whole insert block for the VALUES clause:
        "(<http://example.com/Obama> <http://example.com/president_of> <http://example.com/UnitedStates) 
        (<http://example.com/Hamilton>' '<http://example.com/occupation>' '<http://example.com/Formel1Driver)"

        :param triples: A list of list of triples in n3 syntax.
        :param prefixes: Prefixes that are used within :param triples.
        :param timestamp: If a timestamp is given, the outdated triples will be annotated with this timestamp.
        :return:
        """

        if len(triples) == 0:
            logging.info("List is empty. No triples will be outdated.")
            return

        if prefixes:
            sparql_prefixes = versioning_prefixes(prefixes)
        else:
            sparql_prefixes = versioning_prefixes("")

        logging.info("Creating outdate statement.")
        statement = open(self._template_location + "/outdate_triples.txt", "r").read()

        if isinstance(triples, list):
            logging.info("Creating outdate statement:Build outdate block.")
            for i, line in enumerate(triples):
                triples[i] = line[:-2]
            outdate_list = list(map(list, zip(['('] * len(triples), triples, [')'] * len(triples))))
            outdate_block = list(map(' '.join, outdate_list))
        elif isinstance(triples, str):
            logging.info("Creating outdate statement: Build outdate block.")
            outdate_block = triples.splitlines()
        else:
            raise Exception("Type of triples must be either list or string. See doc of this function.")
        
        logging.info("Outdating triples as batches of 1000 triples.")
        for i in range(0, len(outdate_block), 1000):
            outdate_batch = "\n".join(outdate_block[i:min(i+1000, len(outdate_block))])
            # Surround blank nodes in the subject position with pointy brackets
            outdate_batch = re.sub(r'(?<=^\(\s)_:([a-zA-Z0-9]+)', r'<_:\1>', outdate_batch)
            # Surround blank nodes in the object position with pointy brackets
            outdate_batch = re.sub(r'_:([a-zA-Z0-9]+)\s*(?=\)(\s|$))', r'<_:\1>', outdate_batch)
            if timestamp:
                version_timestamp = versioning_timestamp_format(timestamp)
                outdate_statement = statement.format(sparql_prefixes, outdate_batch, '"' + version_timestamp + '"')
            else:
                outdate_statement = statement.format(sparql_prefixes, outdate_batch, "NOW()")
            self.sparql_post.setQuery(outdate_statement)
            self.sparql_post.query()
        logging.info("Triples outdated.")

    
