from .exceptions import ReservedPrefixError
import re
from typing import Union


def add_versioning_prefixes(prefixes: Union[dict[str, str], str]) -> str:
    """
    Extends the given prefixes by 
        vers: <https://github.com/GreenfishK/DataCitation/versioning/
        xsd: <http://www.w3.org/2001/XMLSchema#>. 
        
    If vers is already contained in :prefixes it will raise an error. If xsd is already 
    contained in :prefixes the returned prologue string will contain the existing xsd prefix and its URI namespace.
    
    :param prefixes: prologue either as dict or str. Every entry of a dict must contain 
    the prefix name as key and the namespace URI as value. If :prefixes is a string 
    it must follow the SPARQL 1.1 prologue syntax.
    :return: The prologue string with the given :prefixes extended by vers and xsd and their corresponding namespace URIs.
    """

    error_message = 'The prefix "citing" is reserved. Please choose another one.'
    prefix_vers = 'PREFIX vers: <https://github.com/GreenfishK/DataCitation/versioning/>'
    prefix_xsd = 'PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>'

    if isinstance(prefixes, dict):
        sparql_prefixes = ""
        for key, value in prefixes.items():
            sparql_prefixes += "PREFIX {0}: <{1}> \n".format(key, value)

        if "vers" in prefixes:
            raise ReservedPrefixError(error_message)
        if "xsd" in prefixes:
            vers_prfx = prefix_vers + "\n"
        else:
            vers_prfx = prefix_vers + "\n" + prefix_xsd + "\n"
        return sparql_prefixes + "\n" + vers_prfx

    else:
        sparql_prefixes = prefixes
        if prefixes.find("vers:") > -1:
            raise ReservedPrefixError(error_message)
        if prefixes.find("xsd:") > -1:
            vers_prfx = prefix_vers + "\n"
        else:
            vers_prfx = prefix_vers + "\n" + prefix_xsd + "\n"
        return sparql_prefixes + "\n" + vers_prfx


def split_prefixes_query(query: str) -> tuple[str, str]:
    """
    Separates the prologue (prefixes at the beginning of the query) from the actual query. 
    If there is no prolog, the prefixes variable will be an empty string.

    :param query: A query string with or without prologue.
    :return: A tuple with the prefixes as the first element and the actual query string as the second element.
    """
    pattern = "PREFIX\\s*[a-zA-Z0-9_-]*:\\s*<.*>\\s*"

    prefixes_list = re.findall(pattern, query, re.MULTILINE)
    prefixes = ''.join(prefixes_list)
    query_without_prefixes = re.sub(pattern, "", query)

    return prefixes, query_without_prefixes
