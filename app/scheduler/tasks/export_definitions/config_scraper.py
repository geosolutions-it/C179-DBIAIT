import re
import json
import schema
import logging

from enum import Enum
from typing import List, Dict
from pypika import Query, Table, Field

from django.conf import settings
from app.scheduler.utils import COMPARISON_OPERATORS_MAPPING

from .exceptions import ExportConfigError
from .transformations import SUPPORTED_TRANSFORMATIONS, TransformationFactory
from .validations import SUPPORTED_VALIDATIONS, ValidationFactory

logger = logging.getLogger(__name__)


class JoinType(Enum):
    # class overriding pypika.enums.JoinType toi extend it for SPATIAL join
    inner = ""
    left = "LEFT"
    right = "RIGHT"
    outer = "FULL OUTER"
    spatial = "SPATIAL"


JOIN_TYPES = [join_type.name.upper() for join_type in JoinType]


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
            schema.Optional("pre_process"): str,
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
                        schema.Optional("group_by"): [str],
                        schema.Optional("filter"): schema.And(str, len),
                        schema.Optional("having"): schema.And(str, len),
                    },
                )
            ],
            "columns": [
                {
                    "id": schema.And(schema.Or(str, int), lambda v: len(str(v)) > 0),
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

            if sheet['skip']:
                continue

            # parse SQL sources of a sheet
            sources = sheet.pop("sources")
            sql_sources = self.parse_sources(sheet["sheet"], sources)
            sheet.update({"sql_sources": sql_sources})

            # parse and update columns of a sheet
            columns = sheet.pop("columns")
            parsed_columns = self.parse_columns(columns)
            sheet.update({"columns": parsed_columns})

            # store current sheet configuration
            self.config.append(sheet)

    def __iter__(self):
        return ExportConfigIterator(self)

    def __len__(self):
        return len(self.config)

    def parse_sources(self, sheet_name: str, sources: List[Dict]) -> List[Dict]:
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
                            join_on_fields.append(Field(field_name, table=join_table))
                        elif table_name == table.get_table_name():
                            join_on_fields.append(Field(field_name, table=table))
                        else:
                            raise ExportConfigError(
                                f'On field name "{field}" in sheet "{sheet_name}" '
                                f'does not recognize table "{table_name}".'
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
                            group_by_field
                        )
                    )

                query = query.select(*fields)

                # add RAW statements provided by a user
                for raw_statement in [
                    source.get("filter", ""),
                    source.get("having", ""),
                ]:
                    if raw_statement:
                        query = str(query) + " " + raw_statement

                # append SQL query string to the configuration sources
                sql_sources.append(str(query))

        return sql_sources

    def parse_columns(self, original_columns: List[Dict]):
        columns = []
        for column in original_columns:
            # translate transformation into parametrized transformer instance
            transformation = column.pop("transformation")
            transformer = TransformationFactory.from_name(
                transformation["func"], transformation["params"]
            )
            column.update({"transformer": transformer})

            # translate validations into parametrized validator instances
            validators = []
            for validation in column.pop("validations", []):
                validator = ValidationFactory.from_name(
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

        return columns


class ExportConfigIterator:
    def __init__(self, export_config):
        self._export_config = export_config
        self._index = 0

    def __next__(self):
        if self._index < len(self._export_config.config):
            result = self._export_config.config[self._index]
            self._index += 1
            return result

        raise StopIteration
