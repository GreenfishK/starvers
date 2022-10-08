from ._exceptions import ReservedPrefixError
from ._helper import template_path
import re


def _prefixes_to_sparql(prefixes: dict) -> str:
    """
    Converts a dict of prefixes to a string with SPARQL syntax for prefixes.
    :param prefixes:
    :return: SPARQL prologue (prefixes at the beginning of the query) as string.
    """
    if prefixes is None:
        return ""

    sparql_prefixes = ""
    for key, value in prefixes.items():
        sparql_prefixes += "PREFIX {0}: <{1}> \n".format(key, value)
    return sparql_prefixes


def attach_prefixes(query: str, prefixes: dict) -> str:
    """
    Attaches the prologue in SPARQL syntax to the SPARQL query. The passed query should therefore have no prologue.

    :param query: A query without prologue (Prefixes at the beginning of the query)
    :param prefixes: Prologue that comes before the query.
    :return: A full query including the prologue
    """
    template = open(template_path("templates/query_utils/prefixes_query_wrapper.txt"), "r").read()
    sparql_prefixes = _prefixes_to_sparql(prefixes)
    query_with_prefixes = template.format(sparql_prefixes, query)
    return query_with_prefixes


def versioning_prefixes(prefixes: dict or str) -> str:
    """
    Extends the given prefixes by citing: <http://ontology.ontotext.com/citing/>
    and xsd: <http://www.w3.org/2001/XMLSchema#>. While citing is reserved and cannot be overwritten by a user prefix
    xsd will be overwritten if a prefix 'xsd' exists in 'prefixes'.
    :param prefixes:
    :return:
    """
    error_message = 'The prefix "citing" is reserved. Please choose another one.'
    prefix_citing = 'PREFIX vers: <https://github.com/GreenfishK/DataCitation/versioning/>'
    prefix_xsd = 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>'

    if isinstance(prefixes, dict):
        sparql_prefixes = _prefixes_to_sparql(prefixes)
        if "vers" in prefixes:
            raise ReservedPrefixError(error_message)
        if "xsd" in prefixes:
            vers_prfx = prefix_citing + "\n"
        else:
            vers_prfx = prefix_citing + "\n" + prefix_xsd + "\n"
        return sparql_prefixes + "\n" + vers_prfx

    if isinstance(prefixes, str):
        sparql_prefixes = prefixes
        if prefixes.find("vers:") > -1:
            raise ReservedPrefixError(error_message)
        if prefixes.find("xsd:") > -1:
            vers_prfx = prefix_citing + "\n"
        else:
            vers_prfx = prefix_citing + "\n" + prefix_xsd + "\n"
        return sparql_prefixes + "\n" + vers_prfx


def split_prefixes_query(query: str) -> list:
    """
    Separates the prologue (prefixes at the beginning of the query) from the actual query. 
    If there is no prolog, the prefixes variable will be an empty string.

    :param query: A query string with or without prologue.
    :return: A list with the prefixes as the first element and the actual query string as the second element.
    """
    pattern = "PREFIX\\s*[a-zA-Z0-9_-]*:\\s*<.*>\\s*"

    prefixes_list = re.findall(pattern, query, re.MULTILINE)
    prefixes = ''.join(prefixes_list)
    query_without_prefixes = re.sub(pattern, "", query, re.MULTILINE)

    return [prefixes, query_without_prefixes]
