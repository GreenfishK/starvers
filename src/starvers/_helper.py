import os
from datetime import datetime


def template_path(template_rel_path: str):
    return os.path.join(os.path.dirname(__file__), template_rel_path)


def versioning_timestamp_format(citation_timestamp: datetime) -> str:
    """
    This format is taken from the result set of GraphDB's queries.
    :param citation_timestamp:
    :return:
    """
    return citation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f%z")[:-2] + ":" + citation_timestamp.strftime("%z")[3:5]
