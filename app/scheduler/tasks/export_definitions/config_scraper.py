import re
import json
import schema
import logging

from pathlib import Path
from typing import List, Dict
from pypika import Query, Table, Field, JoinType

from django.conf import settings
from app.scheduler.utils import COMPARISON_OPERATORS_MAPPING

from .exceptions import ExportConfigError
from .transformations import SUPPORTED_TRANSFORMATIONS, TransformationFactory
from .validations import SUPPORTED_VALIDATIONS, ValidationFactory
from .sql_functions import SUPPORTED_SQL_FUNCTIONS, SQL_FUNCTION_MAPPING, SQL_SPATIAL_JOIN_MAPPING

logger = logging.getLogger(__name__)


JOIN_TYPES = [join_type.name.upper() for join_type in JoinType]


def field_name_validator(field_name: str) -> bool:
    """
    Simple function validating string, matching <table_name>.<field_name> pattern
    """
    return True if re.match("^[A-Za-z0-9_]+.[A-Za-z0-9_]+$", field_name) else False


export_config_schema = schema.Schema(
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
                            schema.Optional("function"): schema.And(
                                str, lambda f: f.upper() in SUPPORTED_SQL_FUNCTIONS
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
                                lambda v: v in list(COMPARISON_OPERATORS_MAPPING.keys()) + list(SQL_SPATIAL_JOIN_MAPPING.keys()),
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
    }
)


class ExportConfig:
    def __init__(self):
        self.config = []

        with open(settings.EXPORT_CONF_FILE.substitute(), "r") as ecf:
            config = json.load(ecf)

        sheets_config_files = config.get("xls_sheet_configs", None)

        if sheets_config_files is None:
            raise ExportConfigError(
                'Export configuration file for export does not define "xls_sheet_configs" key.'
            )

        # parse export configuration
        for sheet_config_file in sheets_config_files:

            sheet_config_path = Path(sheet_config_file)

            if sheet_config_path.is_absolute():
                raise ExportConfigError(
                    f'Sheet config path may not be absolute: {sheet_config_path}.'
                )
            else:
                sheet_config_path = Path(
                    Path(settings.EXPORT_CONF_FILE.substitute()).parent, sheet_config_path
                )

            if not sheet_config_path.exists():
                raise ExportConfigError(
                    f'Sheet configuration file "{sheet_config_path}" does not exist.'
                )

            try:
                with open(sheet_config_path, "r") as scp:
                    sheet = json.load(scp)
            except Exception as e:
                raise ExportConfigError(
                    f'Error occurred while parsing sheet config "{sheet_config_path}":\n'
                    f"{type(e).__name__}: {e}"
                )

            # validate export configuration schema
            export_config_schema.validate(sheet)

            if sheet["skip"]:
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

                fields = []
                for field in source["fields"]:
                    function = field.get("function", None)

                    if function is None:
                        fields.append(
                            Field(
                                field["name"].split(".")[1],
                                table=Table(field["name"].split(".")[0]),
                            ).as_(field.get("alias", field["name"]))
                        )

                    else:
                        f = Field(
                            field["name"].split(".")[1],
                            table=Table(field["name"].split(".")[0]),
                        )
                        fields.append(
                            SQL_FUNCTION_MAPPING[function](f).as_(
                                field.get("alias", field["name"])
                            )
                        )

                query = Query.from_(table)

                # parse tables to join
                for join_table_config in source.get("join", []):
                    join_table = Table(join_table_config["table"]["name"]).as_(
                        join_table_config["table"].get(
                            "alias", join_table_config["table"]["name"]
                        )
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
                                f'ON field name "{field}" in sheet "{sheet_name}" '
                                f'does not recognize table "{table_name}".'
                            )

                    if join_table_config["cond"] in COMPARISON_OPERATORS_MAPPING.keys():
                        on_cond = COMPARISON_OPERATORS_MAPPING[join_table_config["cond"]](
                            join_on_fields[0], join_on_fields[1]
                        )
                    else:
                        on_cond = SQL_SPATIAL_JOIN_MAPPING[join_table_config["cond"]](
                            join_on_fields[0], join_on_fields[1]
                        )

                    query = query.join(
                        join_table, JoinType[join_table_config["type"].lower()]
                    ).on(
                        on_cond
                    )

                # parse GROUP BY parameters
                for group_by_field in source.get("group_by", []):
                    query = query.groupby(Field(group_by_field))

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
