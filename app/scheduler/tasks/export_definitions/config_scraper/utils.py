import re
import schema

from pypika import JoinType

from app.scheduler.utils import COMPARISON_OPERATORS_MAPPING
from .sql_functions import SUPPORTED_SQL_FUNCTIONS, SQL_SPATIAL_JOIN_MAPPING, SUPPORTED_SQL_CASTS

JOIN_TYPES = [join_type.name.upper() for join_type in JoinType]


def field_name_validator(field_name: str) -> bool:
    """
    Simple function validating string, matching <table_name>.<field_name> pattern
    """
    return True if re.match("^[A-Za-z0-9_]+.[A-Za-z0-9_]+$", field_name) else False


config_sources_schema = schema.Or(
    {
        "raw": schema.And(str, len),
    },
    {
        "table": {
            "name": schema.And(str, len),
            schema.Optional("alias"): schema.And(str, len),
        },
        "fields": [
            {
                "name": schema.And(str, field_name_validator),
                schema.Optional("alias"): schema.And(str, len),
                schema.Optional("function"): schema.And(
                    str, lambda f: f.upper() in SUPPORTED_SQL_FUNCTIONS
                ),
                schema.Optional("cast"): schema.And(
                    str, lambda f: True if 'VARCHAR' in f.upper() else f.upper() in SUPPORTED_SQL_CASTS
                ),
            }
        ],
        schema.Optional("join"): [
            {
                "type": schema.And(str, lambda t: t.upper() in JOIN_TYPES),
                "table": {
                    "name": schema.And(str, len),
                    schema.Optional("alias"): schema.And(str, len),
                },
                "on": schema.And(
                    [schema.And(str, field_name_validator)],
                    lambda l: len(l) == 2,
                ),
                "cond": schema.And(
                    str,
                    lambda v: v
                    in list(COMPARISON_OPERATORS_MAPPING.keys())
                    + list(SQL_SPATIAL_JOIN_MAPPING.keys()),
                ),
            }
        ],
        schema.Optional("group_by"): [str],
        schema.Optional("filter"): schema.And(str, len),
        schema.Optional("having"): schema.And(str, len),
    },
)
