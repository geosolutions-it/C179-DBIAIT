from typing import List, Dict
from pypika import Query, Table, Field, JoinType

from app.scheduler.utils import COMPARISON_OPERATORS_MAPPING
from app.scheduler.tasks.export_definitions.exceptions import ExportConfigError
from .sql_functions import SQL_FUNCTION_MAPPING, SQL_SPATIAL_JOIN_MAPPING


class BaseExportConfig:
    def __init__(self):
        self.config = []

    def __iter__(self):
        return ExportConfigIterator(self)

    def __len__(self):
        return len(self.config)

    def parse_sources(self, config_name: str, sources: List[Dict]) -> List[Dict]:
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
                                f'ON field name "{field}" in configuration for "{config_name}" '
                                f'does not recognize table "{table_name}".'
                            )

                    if join_table_config["cond"] in COMPARISON_OPERATORS_MAPPING.keys():
                        on_cond = COMPARISON_OPERATORS_MAPPING[
                            join_table_config["cond"]
                        ](join_on_fields[0], join_on_fields[1])
                    else:
                        on_cond = SQL_SPATIAL_JOIN_MAPPING[join_table_config["cond"]](
                            join_on_fields[0], join_on_fields[1]
                        )

                    query = query.join(
                        join_table, JoinType[join_table_config["type"].lower()]
                    ).on(on_cond)

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
