import os
from datetime import datetime
import pandas as pd
from SPARQLWrapper import Wrapper
import logging
from rdflib.plugins.sparql.parserutils import CompValue

def template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)


def versioning_timestamp_format(citation_timestamp: datetime) -> str:
    """
    This format is taken from the result set of GraphDB's queries.
    :param citation_timestamp:
    :return:
    """
    return citation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + citation_timestamp.strftime("%z")[3:5]


def to_df(result: Wrapper.QueryResult) -> pd.DataFrame:
    """

    :param result:
    :return: Dataframe
    """
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    def format_value(res_value):
        value = res_value["value"]
        lang = res_value.get("xml:lang", None)
        datatype = res_value.get("datatype", None)
        if lang is not None:
            value += "@" + lang
        if datatype is not None:
            value += " [" + datatype + "]"
        return value

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

def pprintAlgebra(q):
    def pp(p, ind="    "):
        if not isinstance(p, CompValue):
            logging.info(p)
            return
        logging.info("{0}(".format(p.name))

        for k in p:
            logging.info("{0}{1} =".format(str(ind), str(k)))
            logging.info(' ')
            pp(p[k], ind + "    ")
        logging.info("{0})".format(ind))

    try:
        pp(q.algebra)
    except AttributeError:
        # it's update, just a list
        for x in q:
            pp(x)
