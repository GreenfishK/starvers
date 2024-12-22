import os
from datetime import datetime
import pandas as pd
from SPARQLWrapper import Wrapper
import logging
from rdflib.plugins.sparql.parserutils import CompValue

logger = logging.getLogger(__name__)

__all__ = ['_template_path', '_versioning_timestamp_format', '_to_df', '_pprintAlgebra']


def _template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)


def _versioning_timestamp_format(version_timestamp: datetime) -> str:
    """
    This format is taken from the result set of GraphDB's queries.
    :param version_timestamp:
    :return:
    """
    if version_timestamp.strftime("%z") != '':
        return version_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]  + version_timestamp.strftime("%z")[0:3] + ":" + version_timestamp.strftime("%z")[3:5]
    else:
        return version_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _to_df(result: Wrapper.QueryResult) -> pd.DataFrame:
    """
    :param result:
    :return: Dataframe
    """
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    
    def format_value(res_value):
        """
        Formats a SPARQL result value while handling proper escaping and selecting suitable quotes.
        
        :param res_value: The value to format, including optional language or datatype information.
        :return: A properly formatted and escaped query result.
        """
    
        def escape_string(value):
            """
            Escapes special characters and selects the appropriate quotes.
            TODO handle escaping
            """
            if '\n' in value or '\r' in value or ('"' in value and "'" in value):
                # Multi-line or contains both single and double quotes
                return f'"""{value}"""'
            elif '"' in value:
                # Contains double quotes, use single quotes
                return f"'{value}'"
            else:
                # Default to double quotes
                return f'"{value}"'
        
        value = res_value["value"]
        lang = res_value.get("xml:lang")
        datatype = res_value.get("datatype")


        if lang:  # Language-tagged literal
            return f"\"{value}\"@{lang}"
        elif datatype:  # Typed literal
            return f"\"{value}\"^^<{datatype}>"
        else:  # Plain string literal or URI
            if res_value["type"] == "uri":
                return f"<{value}>"
            else:
                return escape_string(value)
        
    results = result.convert()

    column_names = []
    for var in results["head"]["vars"]:
        column_names.append(var)
    df = pd.DataFrame(columns=column_names)

    values = []
    for r in results["results"]["bindings"]:
        row = []
        for col in results["head"]["vars"]:
            if col in r:
                result_value = format_value(r[col])
            else:
                result_value = None
            row.append(result_value)
        values.append(row)
    df = df.append(pd.DataFrame(values, columns=df.columns))

    return df

def _pprintAlgebra(q):
    def pp(p, ind="    "):
        if not isinstance(p, CompValue):
            logger.info(p)
            return
        logger.info("{0}(".format(p.name))

        for k in p:
            logger.info("{0}{1} =".format(str(ind), str(k)))
            logger.info(' ')
            pp(p[k], ind + "    ")
        logger.info("{0})".format(ind))

    try:
        pp(q.algebra)
    except AttributeError:
        # it's update, just a list
        for x in q:
            pp(x)
