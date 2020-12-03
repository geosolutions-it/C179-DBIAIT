import json
import schema

from pathlib import Path
from django.conf import settings

from app.scheduler.tasks.export_definitions.exceptions import ExportConfigError
from .config_scraper_base import BaseExportConfig
from .utils import config_sources_schema


shp_export_config_schema = schema.Schema(
    {
        "file_name": schema.And(str, len),
        "skip": bool,
        schema.Optional("pre_process"): str,
        schema.Optional("folder"): schema.And(str, len),
        "sources": [config_sources_schema],
    }
)


class ShpExportConfig(BaseExportConfig):
    def __init__(self):
        super().__init__()

        with open(settings.SHAPEFILE_EXPORT_CONFIG.substitute(), "r") as ecf:
            config = json.load(ecf)

        shapes_config_files = config.get("shp_files_configs", None)

        if shapes_config_files is None:
            raise ExportConfigError(
                'Export configuration file for export does not define "shp_files_configs" key.'
            )

        # parse export configuration
        for shape_config_file in shapes_config_files:

            shape_config_path = Path(shape_config_file)

            if shape_config_path.is_absolute():
                raise ExportConfigError(
                    f"Sheet config path may not be absolute: {shape_config_path}."
                )
            else:
                shape_config_path = Path(
                    Path(settings.EXPORT_CONF_FILE.substitute()).parent,
                    shape_config_path,
                )

            if not shape_config_path.exists():
                raise ExportConfigError(
                    f'Sheet configuration file "{shape_config_path}" does not exist.'
                )

            try:
                with open(shape_config_path, "r") as scp:
                    shape_conf = json.load(scp)
            except Exception as e:
                raise ExportConfigError(
                    f'Error occurred while parsing sheet config "{shape_config_path}":\n'
                    f"{type(e).__name__}: {e}"
                )

            # validate export configuration schema
            shp_export_config_schema.validate(shape_conf)

            if shape_conf["skip"]:
                continue

            # parse SQL sources of a sheet
            sources = shape_conf.pop("sources")
            sql_sources = self.parse_sources(shape_conf["file_name"], sources)
            shape_conf.update({"sql_sources": sql_sources})

            # store current sheet configuration
            self.config.append(shape_conf)
