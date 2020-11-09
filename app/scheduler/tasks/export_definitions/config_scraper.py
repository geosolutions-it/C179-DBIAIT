import json
from enum import Enum
import schema
import logging
import re
from pypika import Query, Table, Field

from django.conf import settings
from app.scheduler.tasks.export_definitions.transformations import TransformationFactory
from app.scheduler.tasks.export_definitions.exceptions import ExportConfigError
from app.scheduler.utils import COMPARISON_OPERATORS_MAPPING

logger = logging.getLogger(__name__)


class JoinType(Enum):
    # class overriding pypika.enums.JoinType to extend it for SPATIAL join
    inner = ""
    left = "LEFT"
    right = "RIGHT"
    outer = "FULL OUTER"
    spatial = "SPATIAL"


JOIN_TYPES = [join_type.name.upper() for join_type in JoinType]
SUPPORTED_TRANSFORMATIONS = [
    "EMPTY",
    "CONST",
    "DIRECT",
    "DOMAIN",
    "LSTRIP",
    "EXPR",
    "IF",
    "CASE",
]
SUPPORTED_VALIDATIONS = ["IF"]


def field_name_validator(field_name: str) -> bool:
    """
    Simple function validating string, matching <table_name>.<field_name> pattern
    """
    return True if re.match("^[A-Za-z0-9_]+.[A-Za-z0-9_]+$", field_name) else False


export_config_schema = schema.Schema(
    [
        {
            "sheet": schema.And(str, len),
            "skip": bool,
            "pre_process": schema.Optional(str),
            "sources": [
                schema.Or(
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
                            }
                        ],
                        schema.Optional("join"): [
                            {
                                "type": schema.And(
                                    str, lambda t: t.upper() in JOIN_TYPES
                                ),
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
                                    lambda v: v in COMPARISON_OPERATORS_MAPPING.keys(),
                                ),
                            }
                        ],
                        schema.Optional("group_by"): [
                            schema.And(str, field_name_validator)
                        ],
                        schema.Optional("filter"): schema.And(str, len),
                        schema.Optional("having"): schema.And(str, len),
                    },
                    only_one=True,
                )
            ],
            "columns": [
                {
                    "id": schema.And(schema.Or(str, int), lambda v: len(str(v)) > 0),
                    "field": schema.And(str, len),
                    "transformation": {
                        "func": schema.And(
                            str, lambda v: v.upper() in SUPPORTED_TRANSFORMATIONS
                        ),
                        schema.Optional("params"): {str: object},
                    },
                    schema.Optional("validations"): [
                        {
                            "func": schema.And(
                                str, lambda v: v.upper() in SUPPORTED_VALIDATIONS
                            ),
                            schema.Optional("params"): {str: object},
                            schema.Optional("warning"): str,
                        }
                    ],
                }
            ],
        },
    ]
)


class ExportConfig:
    def __init__(self):
        self.config = []

        with open(settings.EXPORT_CONF_FILE, "r") as ecf:
            config = json.load(ecf)

        # validate export configuration schema
        export_config_schema.validate(config)

        # parse export configuration
        for sheet in config:

            sources = sheet.pop("sources")

            # parse SQL sources of a sheet
            sql_sources = []
            for source in sources:

                if source.get("raw", None) is not None:
                    sql_sources.append(source["raw"])

                else:
                    # sql query builder
                    table = Table(source["table"]["name"]).as_(
                        source["table"].get("alias", source["table"]["name"])
                    )
                    fields = [
                        Field(
                            field["name"].split(".")[1],
                            table=Table(field["name"].split(".")[0]),
                        ).as_(field.get("alias", field["name"]))
                        for field in source["fields"]
                    ]
                    query = Query.from_(table)

                    # parse tables to join
                    for join_table_config in source.get("join", []):
                        join_table = Table(join_table_config["table"]["name"]).as_(
                            join_table_config["table"].get("alias", "name")
                        )

                        # parse fields to join the table on
                        join_on_fields = []
                        for field in join_table_config["on"]:
                            table_name, field_name = field.split(".")
                            if table_name == join_table.get_table_name():
                                join_on_fields.append(
                                    Field(field_name, table=join_table)
                                )
                            elif table_name == table.get_table_name():
                                join_on_fields.append(Field(field_name, table=table))
                            else:
                                raise ExportConfigError(
                                    f'On field name "{field}" in sheet "{sheet}" does not recognize table "{table_name}".'
                                )

                        query = query.join(
                            join_table, JoinType[join_table_config["type"].lower()]
                        ).on(
                            COMPARISON_OPERATORS_MAPPING[join_table_config["cond"]](
                                join_on_fields[0], join_on_fields[1]
                            )
                        )

                    # parse GROUP BY parameters
                    for group_by_field in source.get("group_by", []):
                        query = query.groupby(
                            Field(
                                group_by_field.split(".")[1],
                                table=Table(group_by_field.split(".")[0]),
                            )
                        )

                    query = query.select(fields)

                    # add RAW statements provided by a user
                    for raw_statement in [
                        source.get("filter", ""),
                        source.get("having", ""),
                    ]:
                        if raw_statement:
                            query = str(query) + " " + raw_statement

                    # append SQL query string to the configuration sources
                    sql_sources.append(str(query))

            sheet.update({"sql_sources": sql_sources})

            columns = []
            for column in sheet.pop("columns"):
                # translate transformation into parametrized transformer instance
                transformation = column.pop("transformation")
                transformer = TransformationFactory.from_name(
                    transformation["func"], transformation["params"]
                )
                column.update({"transformer": transformer})

                # translate validations into parametrized validator instances
                validators = []
                for validation in column.pop("validations"):
                    validator = TransformationFactory.from_name(
                        validation["func"], validation["params"]
                    )
                    validators.append(
                        {
                            "validator": validator,
                            "warning": validation.get("warning", None),
                        }
                    )

                column.update({"validators": validators})
                # update translated columns list
                columns.append(column)

            sheet.update({"columns": columns})

            # store current sheet configuration
            self.config.append(sheet)
