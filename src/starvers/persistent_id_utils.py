from ._helper import template_path, versioning_timestamp_format
from .prefixes import split_prefixes_query, versioning_prefixes
from ._exceptions import ExpressionNotCoveredException, InputMissing

from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.term import Variable, Identifier, URIRef
from rdflib.paths import SequencePath, Path, NegatedPath, AlternativePath, InvPath, MulPath, ZeroOrOne, \
    ZeroOrMore, OneOrMore
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
import rdflib.plugins.sparql.parser
from rdflib.plugins.sparql.operators import TrueFilter, UnaryNot, Builtin_BOUND
import pandas as pd
from pandas.util import hash_pandas_object
import hashlib
import datetime
import json
import numpy as np
from datetime import datetime, timedelta, timezone
import tzlocal
import os
import logging
import collections



def _translate_algebra(query_algebra: rdflib.plugins.sparql.sparql.Query = None) -> str:
    """

    :param query_algebra: An algebra returned by the function call algebra.translateQuery(parse_tree)
    from the rdflib library.
    :return: The query form of the algebra.
    """

    def overwrite(text):
        file = open("query.txt", "w+")
        file.write(text)
        file.close()

    def replace(old, new, search_from_match: str = None, search_from_match_occurrence: int = None, count: int = 1):
        # Read in the file
        with open('query.txt', 'r') as file:
            query = file.read()

        def find_nth(haystack, needle, n):
            start = haystack.lower().find(needle)
            while start >= 0 and n > 1:
                start = haystack.lower().find(needle, start + len(needle))
                n -= 1
            return start

        if search_from_match and search_from_match_occurrence:
            position = find_nth(query, search_from_match, search_from_match_occurrence)
            query_pre = query[:position]
            query_post = query[position:].replace(old, new, count)
            query = query_pre + query_post
        else:
            query = query.replace(old, new, count)

        # Write the file out again
        with open('query.txt', 'w') as file:
            file.write(query)

    def convert_node_arg(node_arg):
        if isinstance(node_arg, Identifier):
            if node_arg in aggr_vars.keys() and len(aggr_vars[node_arg]) > 0:
                grp_var = aggr_vars[node_arg].pop(0)
                return grp_var.n3()
            else:
                return node_arg.n3()
        elif isinstance(node_arg, CompValue):
            return "{" + node_arg.name + "}"
        elif isinstance(node_arg, Expr):
            return "{" + node_arg.name + "}"
        elif isinstance(node_arg, str):
            return node_arg
        else:
            raise ExpressionNotCoveredException(
                "The expression {0} might not be covered yet.".format(node_arg))

    aggr_vars = collections.defaultdict(list)

    def sparql_query_text(node):
        """
         https://www.w3.org/TR/sparql11-query/#sparqlSyntax

        :param node:
        :return:
        """

        if isinstance(node, CompValue):
            # 18.2 Query Forms
            if node.name == "SelectQuery":
                overwrite("-*-SELECT-*- " + "{" + node.p.name + "}")

            # 18.2 Graph Patterns
            elif node.name == "BGP":
                # Identifiers or Paths
                # Negated path throws a type error. Probably n3() method of negated paths should be fixed
                triples = "".join(triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3() + "."
                                  for triple in node.triples)
                replace("{BGP}", triples)
                # The dummy -*-SELECT-*- is placed during a SelectQuery or Multiset pattern in order to be able
                # to match extended variables in a specific Select-clause (see "Extend" below)
                replace("-*-SELECT-*-", "SELECT", count=-1)
                # If there is no "Group By" clause the placeholder will simply be deleted. Otherwise there will be
                # no matching {GroupBy} placeholder because it has already been replaced by "group by variables"
                replace("{GroupBy}", "", count=-1)
                replace("{Having}", "", count=-1)
            elif node.name == "Join":
                replace("{Join}", "{" + node.p1.name + "}{" + node.p2.name + "}")  #
            elif node.name == "LeftJoin":
                replace("{LeftJoin}", "{" + node.p1.name + "}OPTIONAL{{" + node.p2.name + "}}")
            elif node.name == "Filter":
                if isinstance(node.expr, CompValue):
                    expr = node.expr.name
                else:
                    raise ExpressionNotCoveredException("This expression might not be covered yet.")
                if node.p:
                    # Filter with p=AggregateJoin = Having
                    if node.p.name == "AggregateJoin":
                        replace("{Filter}", "{" + node.p.name + "}")
                        replace("{Having}", "HAVING({" + expr + "})")
                    else:
                        replace("{Filter}", "FILTER({" + expr + "}) {" + node.p.name + "}")
                else:
                    replace("{Filter}", "FILTER({" + expr + "})")

            elif node.name == "Union":
                replace("{Union}", "{{" + node.p1.name + "}}UNION{{" + node.p2.name + "}}")
            elif node.name == "Graph":
                expr = "GRAPH " + node.term.n3() + " {{" + node.p.name + "}}"
                replace("{Graph}", expr)
            elif node.name == "Extend":
                query_string = open('query.txt', 'r').read().lower()
                select_occurrences = query_string.count('-*-select-*-')
                replace(node.var.n3(), "(" + convert_node_arg(node.expr) + " as " + node.var.n3() + ")",
                        search_from_match='-*-select-*-', search_from_match_occurrence=select_occurrences)
                replace("{Extend}", "{" + node.p.name + "}")
            elif node.name == "Minus":
                expr = "{" + node.p1.name + "}MINUS{{" + node.p2.name + "}}"
                replace("{Minus}", expr)
            elif node.name == "Group":
                group_by_vars = []
                if node.expr:
                    for var in node.expr:
                        if isinstance(var, Identifier):
                            group_by_vars.append(var.n3())
                        else:
                            raise ExpressionNotCoveredException("This expression might not be covered yet.")
                    replace("{Group}", "{" + node.p.name + "}")
                    replace("{GroupBy}", "GROUP BY " + " ".join(group_by_vars) + " ")
                else:
                    replace("{Group}", "{" + node.p.name + "}")
            elif node.name == "AggregateJoin":
                replace("{AggregateJoin}", "{" + node.p.name + "}")
                for agg_func in node.A:
                    if isinstance(agg_func.res, Identifier):
                        identifier = agg_func.res.n3()
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")

                    aggr_vars[agg_func.res].append(agg_func.vars)
                    agg_func_name = agg_func.name.split('_')[1]
                    distinct = ""
                    if agg_func.distinct:
                        distinct = agg_func.distinct + " "
                    if agg_func_name == 'GroupConcat':
                        replace(identifier, "GROUP_CONCAT" + "(" + distinct
                                + agg_func.vars.n3() + ";SEPARATOR=" + agg_func.separator.n3() + ")")
                    else:
                        replace(identifier, agg_func_name.upper() + "(" + distinct
                                + convert_node_arg(agg_func.vars) + ")")

                    # For non-aggregated variables the aggregation function "sample" is automatically assigned.
                    # However, we do not want to have "sample" wrapped around non-aggregated variables. That is
                    # why we replace it. If "sample" is used on purpose it will not be replaced as the alias
                    # must be different from the variable in this case.
                    replace("(SAMPLE({0}) as {0})".format(convert_node_arg(agg_func.vars)),
                            convert_node_arg(agg_func.vars))
            elif node.name == "GroupGraphPatternSub":
                replace("GroupGraphPatternSub", " ".join([convert_node_arg(pattern) for pattern in node.part]))
            elif node.name == "TriplesBlock":
                replace("{TriplesBlock}", "".join(triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3() + "."
                                                  for triple in node.triples))

            # 18.2 Solution modifiers
            elif node.name == "ToList":
                raise ExpressionNotCoveredException("This expression might not be covered yet.")
            elif node.name == "OrderBy":
                order_conditions = []
                for c in node.expr:
                    if isinstance(c.expr, Identifier):
                        var = c.expr.n3()
                        if c.order is not None:
                            cond = c.order + "(" + var + ")"
                        else:
                            cond = var
                        order_conditions.append(cond)
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")
                replace("{OrderBy}", "{" + node.p.name + "}")
                replace("{OrderConditions}", " ".join(order_conditions) + " ")
            elif node.name == "Project":
                project_variables = []
                for var in node.PV:
                    if isinstance(var, Identifier):
                        project_variables.append(var.n3())
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")
                order_by_pattern = ""
                if node.p.name == "OrderBy":
                    order_by_pattern = "ORDER BY {OrderConditions}"
                replace("{Project}", " ".join(project_variables) + "{{" + node.p.name + "}}"
                        + "{GroupBy}" + order_by_pattern + "{Having}")
            elif node.name == "Distinct":
                replace("{Distinct}", "DISTINCT {" + node.p.name + "}")
            elif node.name == "Reduced":
                replace("{Reduced}", "REDUCED {" + node.p.name + "}")
            elif node.name == "Slice":
                slice = "OFFSET " + str(node.start) + " LIMIT " + str(node.length)
                replace("{Slice}", "{" + node.p.name + "}" + slice)
            elif node.name == "ToMultiSet":
                if node.p.name == "values":
                    replace("{ToMultiSet}", "{{" + node.p.name + "}}")
                else:
                    replace("{ToMultiSet}", "{-*-SELECT-*- " + "{" + node.p.name + "}" + "}")

            # 18.2 Property Path

            # 17 Expressions and Testing Values
            # # 17.3 Operator Mapping
            elif node.name == "RelationalExpression":
                expr = convert_node_arg(node.expr)
                op = node.op
                if isinstance(list, type(node.other)):
                    other = "(" + ", ".join(convert_node_arg(expr) for expr in node.other) + ")"
                else:
                    other = convert_node_arg(node.other)
                condition = "{left} {operator} {right}".format(left=expr, operator=op, right=other)
                replace("{RelationalExpression}", condition)
            elif node.name == "ConditionalAndExpression":
                inner_nodes = " && ".join([convert_node_arg(expr) for expr in node.other])
                replace("{ConditionalAndExpression}", convert_node_arg(node.expr) + " && " + inner_nodes)
            elif node.name == "ConditionalOrExpression":
                inner_nodes = " || ".join([convert_node_arg(expr) for expr in node.other])
                replace("{ConditionalOrExpression}", "(" + convert_node_arg(node.expr) + " || " + inner_nodes + ")")
            elif node.name == "MultiplicativeExpression":
                left_side = convert_node_arg(node.expr)
                multiplication = left_side
                for i, operator in enumerate(node.op):
                    multiplication += operator + " " + convert_node_arg(node.other[i]) + " "
                replace("{MultiplicativeExpression}", multiplication)
            elif node.name == "AdditiveExpression":
                left_side = convert_node_arg(node.expr)
                addition = left_side
                for i, operator in enumerate(node.op):
                    addition += operator + " " + convert_node_arg(node.other[i]) + " "
                replace("{AdditiveExpression}", addition)
            elif node.name == "UnaryNot":
                replace("{UnaryNot}", "!" + convert_node_arg(node.expr))

            # # 17.4 Function Definitions
            # # # 17.4.1 Functional Forms
            elif node.name.endswith('BOUND'):
                bound_var = convert_node_arg(node.arg)
                replace("{Builtin_BOUND}", "bound(" + bound_var + ")")
            elif node.name.endswith('IF'):
                arg2 = convert_node_arg(node.arg2)
                arg3 = convert_node_arg(node.arg3)

                if_expression = "IF(" + "{" + node.arg1.name + "}, " + arg2 + ", " + arg3 + ")"
                replace("{Builtin_IF}", if_expression)
            elif node.name.endswith('COALESCE'):
                replace("{Builtin_COALESCE}", "COALESCE(" + ", ".join(convert_node_arg(arg) for arg in node.arg) + ")")
            elif node.name.endswith('Builtin_EXISTS'):
                # The node's name which we get with node.graph.name returns "Join" instead of GroupGraphPatternSub
                # According to https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rExistsFunc
                # ExistsFunc can only have a GroupGraphPattern as parameter. However, when we print the query algebra
                # we get a GroupGraphPatternSub
                replace("{Builtin_EXISTS}", "EXISTS " + "{{" + node.graph.name + "}}")
                algebra.traverse(node.graph, visitPre=sparql_query_text)
                return node.graph
            elif node.name.endswith('Builtin_NOTEXISTS'):
                # The node's name which we get with node.graph.name returns "Join" instead of GroupGraphPatternSub
                # According to https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rNotExistsFunc
                # NotExistsFunc can only have a GroupGraphPattern as parameter. However, when we print the query algebra
                # we get a GroupGraphPatternSub
                replace("{Builtin_NOTEXISTS}", "NOT EXISTS " + "{{" + node.graph.name + "}}")
                algebra.traverse(node.graph, visitPre=sparql_query_text)
                return node.graph
            # # # # 17.4.1.5 logical-or: Covered in "RelationalExpression"
            # # # # 17.4.1.6 logical-and: Covered in "RelationalExpression"
            # # # # 17.4.1.7 RDFterm-equal: Covered in "RelationalExpression"
            elif node.name.endswith('sameTerm'):
                replace("{Builtin_sameTerm}", "SAMETERM(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            # # # # IN: Covered in "RelationalExpression"
            # # # # NOT IN: Covered in "RelationalExpression"

            # # # 17.4.2 Functions on RDF Terms
            elif node.name.endswith('Builtin_isIRI'):
                replace("{Builtin_isIRI}", "isIRI(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_isBLANK'):
                replace("{Builtin_isBLANK}", "isBLANK(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_isLITERAL'):
                replace("{Builtin_isLITERAL}", "isLITERAL(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_isNUMERIC'):
                replace("{Builtin_isNUMERIC}", "isNUMERIC(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_STR'):
                replace("{Builtin_STR}", "STR(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_LANG'):
                replace("{Builtin_LANG}", "LANG(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_DATATYPE'):
                replace("{Builtin_DATATYPE}", "DATATYPE(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_IRI'):
                replace("{Builtin_IRI}", "IRI(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_BNODE'):
                replace("{Builtin_BNODE}", "BNODE(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('STRDT'):
                replace("{Builtin_STRDT}", "STRDT(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_STRLANG'):
                replace("{Builtin_STRLANG}", "STRLANG(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_UUID'):
                replace("{Builtin_UUID}", "UUID()")
            elif node.name.endswith('Builtin_STRUUID'):
                replace("{Builtin_STRUUID}", "STRUUID()")

            # # # 17.4.3 Functions on Strings
            elif node.name.endswith('Builtin_STRLEN'):
                replace("{Builtin_STRLEN}", "STRLEN(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_SUBSTR'):
                args = [convert_node_arg(node.arg), node.start]
                if node.length:
                    args.append(node.length)
                expr = "SUBSTR(" + ", ".join(args) + ")"
                replace("{Builtin_SUBSTR}", expr)
            elif node.name.endswith('Builtin_UCASE'):
                replace("{Builtin_UCASE}", "UCASE(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_LCASE'):
                replace("{Builtin_LCASE}", "LCASE(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_STRSTARTS'):
                replace("{Builtin_STRSTARTS}", "STRSTARTS(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_STRENDS'):
                replace("{Builtin_STRENDS}", "STRENDS(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_CONTAINS'):
                replace("{Builtin_CONTAINS}", "CONTAINS(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_STRBEFORE'):
                replace("{Builtin_STRBEFORE}", "STRBEFORE(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_STRAFTER'):
                replace("{Builtin_STRAFTER}", "STRAFTER(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_ENCODE_FOR_URI'):
                replace("{Builtin_ENCODE_FOR_URI}", "ENCODE_FOR_URI(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_CONCAT'):
                expr = 'CONCAT({vars})'.format(vars=", ".join(convert_node_arg(elem) for elem in node.arg))
                replace("{Builtin_CONCAT}", expr)
            elif node.name.endswith('Builtin_LANGMATCHES'):
                replace("{Builtin_LANGMATCHES}", "LANGMATCHES(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('REGEX'):
                args = [convert_node_arg(node.text), convert_node_arg(node.pattern)]
                expr = "REGEX(" + ", ".join(args) + ")"
                replace("{Builtin_REGEX}", expr)
            elif node.name.endswith('REPLACE'):
                replace("{Builtin_REPLACE}", "REPLACE(" + convert_node_arg(node.arg)
                        + ", " + convert_node_arg(node.pattern) + ", " + convert_node_arg(node.replacement) + ")")

            # # # 17.4.4 Functions on Numerics
            elif node.name == 'Builtin_ABS':
                replace("{Builtin_ABS}", "ABS(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_ROUND':
                replace("{Builtin_ROUND}", "ROUND(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_CEIL':
                replace("{Builtin_CEIL}", "CEIL(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_FLOOR':
                replace("{Builtin_FLOOR}", "FLOOR(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_RAND':
                replace("{Builtin_RAND}", "RAND()")

            # # # 17.4.5 Functions on Dates and Times
            elif node.name == 'Builtin_NOW':
                replace("{Builtin_NOW}", "NOW()")
            elif node.name == 'Builtin_YEAR':
                replace("{Builtin_YEAR}", "YEAR(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_MONTH':
                replace("{Builtin_MONTH}", "MONTH(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_DAY':
                replace("{Builtin_DAY}", "DAY(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_HOURS':
                replace("{Builtin_HOURS}", "HOURS(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_MINUTES':
                replace("{Builtin_MINUTES}", "MINUTES(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SECONDS':
                replace("{Builtin_SECONDS}", "SECONDS(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_TIMEZONE':
                replace("{Builtin_TIMEZONE}", "TIMEZONE(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_TZ':
                replace("{Builtin_TZ}", "TZ(" + convert_node_arg(node.arg) + ")")

            # # # 17.4.6 Hash functions
            elif node.name == 'Builtin_MD5':
                replace("{Builtin_MD5}", "MD5(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SHA1':
                replace("{Builtin_SHA1}", "SHA1(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SHA256':
                replace("{Builtin_SHA256}", "SHA256(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SHA384':
                replace("{Builtin_SHA384}", "SHA384(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SHA512':
                replace("{Builtin_SHA512}", "SHA512(" + convert_node_arg(node.arg) + ")")

            # Other
            elif node.name == 'values':
                columns = []
                for key in node.res[0].keys():
                    if isinstance(key, Identifier):
                        columns.append(key.n3())
                    else:
                        raise ExpressionNotCoveredException("The expression {0} might not be covered yet.".format(key))
                values = "VALUES (" + " ".join(columns) + ")"

                rows = ""
                for elem in node.res:
                    row = []
                    for term in elem.values():
                        if isinstance(term, Identifier):
                            row.append(term.n3())  # n3() is not part of Identifier class but every subclass has it
                        elif isinstance(term, str):
                            row.append(term)
                        else:
                            raise ExpressionNotCoveredException(
                                "The expression {0} might not be covered yet.".format(term))
                    rows += "(" + " ".join(row) + ")"

                replace("values", values + "{" + rows + "}")
            elif node.name == 'ServiceGraphPattern':
                replace("{ServiceGraphPattern}", "SERVICE " + convert_node_arg(node.term)
                        + "{" + node.graph.name + "}")
                algebra.traverse(node.graph, visitPre=sparql_query_text)
                return node.graph
            # else:
            #     raise ExpressionNotCoveredException("The expression {0} might not be covered yet.".format(node.name))

    algebra.traverse(query_algebra.algebra, visitPre=sparql_query_text)
    query_from_algebra = open("query.txt", "r").read()
    os.remove("query.txt")

    return query_from_algebra


def _resolve_paths(node: CompValue):
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
                    if triple[1].mod == ZeroOrOne:
                        raise ExpressionNotCoveredException("ZeroOrOne path has not be covered yet. "
                                                            "Path will not be resolved")
                    if triple[1].mod == ZeroOrMore:
                        raise ExpressionNotCoveredException("ZeroOrMore path has not be covered yet. "
                                                            "Path will not be resolved")
                    if triple[1].mod == OneOrMore:
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

        elif node.name == "TriplesBlock":
            raise ExpressionNotCoveredException("TriplesBlock has not been covered yet. "
                                                "If there are any paths they will not be resolved.")


class QueryUtils:

    def __init__(self, query: str, execution_timestamp: datetime = None):
        """
        Initializes the QueryData object and presets most of the variables by calling functions from this class.

        :param query: The SPARQL select statement that is used to retrieve the data set for citing.
        :param execution_timestamp: The timestamp as of q_handler.
        """

        self.sparql_prefixes, self.query = split_prefixes_query(query)

        if execution_timestamp is not None:
            self.execution_timestamp = versioning_timestamp_format(execution_timestamp)  # -> str
        else:
            current_datetime = datetime.now()
            timezone_delta = tzlocal.get_localzone().dst(current_datetime).seconds
            execution_datetime = datetime.now(timezone(timedelta(seconds=timezone_delta)))
            self.execution_timestamp = versioning_timestamp_format(execution_datetime)
        self.timestamped_query = self.timestamp_query()


    def timestamp_query(self, query: str = None, citation_timestamp: datetime = None) -> str:
        """
        R7 - Query timestamping
        Binds a q_handler timestamp to the variable ?TimeOfExecution and wraps it around the query. Also extends
        the query with a code snippet that ensures that a snapshot of the data as of q_handler
        time gets returned when the query is executed. Optionally, but recommended, the order by clause
        is attached to the query to ensure a unique sort of the data.

        :param query:
        :param citation_timestamp:
        Use this parameter only for presentation purpose as the code for color encoding will make the SPARQL
        query erroneous!
        :return: A query string extended with the given timestamp
        """

        if query is None:
            if self.query is not None and self.sparql_prefixes is not None:
                prefixes = self.sparql_prefixes
                query = self.query
            else:
                raise InputMissing("Query could not be normalized because the query string is not set.")
        else:
            prefixes, query = split_prefixes_query(query)
        query_vers = prefixes + "\n" + query

        if citation_timestamp is not None:
            timestamp = versioning_timestamp_format(citation_timestamp)
        else:
            timestamp = self.execution_timestamp

        bgp_triples = {}

        def inject_versioning_extensions(node):
            if isinstance(node, CompValue):
                if node.name == "BGP":
                    bgp_id = "BGP_" + str(len(bgp_triples))
                    bgp_triples[bgp_id] = node.triples.copy()
                    node.triples.append((rdflib.term.Literal('__{0}dummy_subject__'.format(bgp_id)),
                                         rdflib.term.Literal('__{0}dummy_predicate__'.format(bgp_id)),
                                         rdflib.term.Literal('__{0}dummy_object__'.format(bgp_id))))
                elif node.name == "TriplesBlock":
                    raise ExpressionNotCoveredException("TriplesBlock has not been covered yet. "
                                                        "No versioning extensions will be injected.")

        query_tree = parser.parseQuery(query_vers)
        query_algebra = algebra.translateQuery(query_tree)
        algebra.translateAlgebra
        try:
            algebra.traverse(query_algebra.algebra, visitPre=_resolve_paths)
            algebra.traverse(query_algebra.algebra, visitPre=inject_versioning_extensions)
        except ExpressionNotCoveredException as e:
            err = "Query will not be timestamped because of following error: {0}".format(e)
            raise ExpressionNotCoveredException(err)

        query_vers_out = _translate_algebra(query_algebra)
        for bgp_identifier, triples in bgp_triples.items():
            ver_block_template = \
                open(template_path("templates/query_utils/versioning_query_extensions.txt"), "r").read()

            ver_block = ""
            for i, triple in enumerate(triples):
                templ = ver_block_template
                triple_n3 = triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3()
                ver_block += templ.format(triple_n3,
                                          "?triple_statement_{0}_valid_from".format(str(i)),
                                          "?triple_statement_{0}_valid_until".format(str(i)),
                                          bgp_identifier)

            dummy_triple = rdflib.term.Literal('__{0}dummy_subject__'.format(bgp_identifier)).n3() + " "\
                           + rdflib.term.Literal('__{0}dummy_predicate__'.format(bgp_identifier)).n3() + " "\
                           + rdflib.term.Literal('__{0}dummy_object__'.format(bgp_identifier)).n3() + "."
            ver_block += 'bind("{0}"^^xsd:dateTime as ?TimeOfExecution{1})'.format(timestamp, bgp_identifier)
            query_vers_out = query_vers_out.replace(dummy_triple, ver_block)

        query_vers_out = versioning_prefixes("") + "\n" + query_vers_out

        return query_vers_out
