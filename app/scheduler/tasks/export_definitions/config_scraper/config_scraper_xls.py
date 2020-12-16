import json
import schema

from pathlib import Path
from typing import List, Dict

from django.conf import settings

from app.scheduler.tasks.export_definitions.exceptions import ExportConfigError
from app.scheduler.tasks.export_definitions.transformations import (
    SUPPORTED_TRANSFORMATIONS,
    TransformationFactory,
)
from app.scheduler.tasks.export_definitions.validations import (
    SUPPORTED_VALIDATIONS,
    ValidationFactory,
)

from .config_scraper_base import BaseExportConfig
from .utils import config_sources_schema


xls_export_config_schema = schema.Schema(
    {
        "sheet": schema.And(str, len),
        "skip": bool,
        schema.Optional("pre_process"): str,
        "sources": [config_sources_schema],
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


class XlsExportConfig(BaseExportConfig):
    def __init__(self, year=None):
        super().__init__()
        self.year = year

        sett = settings.EXPORT_CONF_FILE.substitute()
        if year is not None:
            sett = settings.EXPORT_CONF_FILE.substitute({"year": year})

        with open(sett, "r") as ecf:
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
                    f"Sheet config path may not be absolute: {sheet_config_path}."
                )
            else:
                parent_folder = Path(settings.EXPORT_CONF_FILE.substitute()).parent
                if year is not None:
                    parent_folder = Path(settings.EXPORT_CONF_FILE.substitute({"year": year})).parent

                sheet_config_path = Path(
                    parent_folder,
                    sheet_config_path,
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
            xls_export_config_schema.validate(sheet)

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
