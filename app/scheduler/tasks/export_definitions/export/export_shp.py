import fiona
import pathlib
import shapely.wkb

from shapely.geometry import mapping
from django.db import connections, ProgrammingError

from app.scheduler.tasks.export_definitions.config_scraper import ShpExportConfig
from app.scheduler.utils import dictfetchall, translate_schema_to_db_alias

from .export_base import ExportBase


class ExportShp(ExportBase):

    def run(self):
        """
        Method executing export of the data into *.xlsx file,
        """
        self.starting_progress = self.orm_task.progress
        self.configure_file_logger()

        # parse export configuration
        config = ShpExportConfig()

        # calculate total number of steps
        total_shapes_number = len(config)
        step = 1

        for shape_conf in config:

            # execute pre_process for the shapefile
            pre_process = shape_conf.get("pre_process", None)
            try:
                self.execute_pre_process(pre_process)
            except Exception as e:
                self.logger.error(
                    f"Procedure '{pre_process}' called by shapefile '{shape_conf['name']}' FAILED with:\n"
                    f"{type(e).__name__}: {e}.\n"
                    f"Skipping '{shape_conf['name']}' sheet generation."
                )
                continue

            # get target shapefile location
            target_shapefile = pathlib.Path(
                self.export_dir.absolute(),
                shape_conf["folder"],
                shape_conf["file_name"],
            ) if shape_conf.get("folder", None) else pathlib.Path(
                self.export_dir.absolute(),
                shape_conf["file_name"] + ".shp",
            )

            target_shapefile.parent.mkdir(parents=True, exist_ok=True)

            with connections[
                translate_schema_to_db_alias(self.orm_task.schema)
            ].cursor() as cursor:
                sql_sources = shape_conf["sql_sources"]

                if not sql_sources:
                    self.logger.warning(
                        f"Sources for '{shape_conf['file_name']}' is empty. Skipping..."
                    )
                    continue

                raw_data = []
                for source in sql_sources:
                    try:
                        cursor.execute(source)
                    except ProgrammingError as e:
                        self.logger.error(
                            f"Fetching source for sheet '{shape_conf['file_name']}' failed with:\n"
                            f"{type(e).__name__}: {e}.\n"
                            f"Source: '{source}'."
                        )
                        continue

                    raw_data.extend(dictfetchall(cursor))

            records = []
            shp_schema = None
            for raw_data_row in raw_data:

                hex_geom = raw_data_row.pop('geom', None)
                if hex_geom is None:
                    self.logger.debug(f"Empty geometry for row in {shape_conf['file_name']}")
                    continue

                geom = shapely.wkb.loads(hex_geom, hex=True)

                if shp_schema is None:
                    shp_schema = {
                        'geometry': type(geom).__name__,
                        'properties': {
                            key: type(val).__name__ for key, val in raw_data_row.items() if key != 'geom'
                        }
                    }

                records.append({
                        'geometry': mapping(geom),
                        'properties': raw_data_row,
                    }
                )

            with fiona.open(str(target_shapefile.absolute()), 'w', 'ESRI Shapefile', shp_schema) as shp:
                shp.writerecords(records)

            # update task status
            step += 1
            self.update_progress(step, total_shapes_number)

        self.set_max_progress()
