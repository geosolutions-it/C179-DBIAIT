import fiona
import pathlib
import shapely.wkb

from django.conf import settings
from shapely.geometry import mapping
from django.db import connections, ProgrammingError
from app.scheduler.tasks.import_definitions.base_import import initQgis
from app.scheduler.tasks.export_definitions.config_scraper import ShpExportConfig
from app.scheduler.utils import dictfetchall, translate_schema_to_db_alias

from .export_base import ExportBase


class ExportShp(ExportBase):

    def create_gdal_commands(self, database_config, shp_full_path, sql, is_windows=False):
        ogr_exe = "/usr/bin/ogr2ogr"
        if is_windows:
            ogr_exe = "ogr2ogr.exe"

        db_host = database_config["HOST"]
        db_port = database_config["PORT"]
        db_name = database_config["DATABASE"]
        db_schema = database_config["SCHEMA"]
        db_user = database_config["USERNAME"]
        db_password = database_config["PASSWORD"]

        options = '-progress '
        options += '-f "ESRI Shapefile" ' + shp_full_path + ' '
        options += 'PG:" dbname=\'%s\' host=%s port=%s user=\'%s\' password=\'%s\' active_schema=%s " ' \
                   % (db_name, db_host, db_port, db_user, db_password, db_schema)
        options += '-sql "%s" ' % (sql,)
        #options += '-lco GEOMETRY_NAME=geom '

        self.logger.debug("OGR Command: " + options)

        commands = [ogr_exe, options]
        return commands

    def execute_command(self, gdal_utils, commands):
        try:
            gdal_utils.runGdal(commands)
            print(gdal_utils.consoleOutput)
        except Exception as e:
            print(e)
            traceback.print_exc()

    def run(self):
        """
        Method executing export of the data into *.shp file,
        """
        database = settings.DATABASES[settings.TASKS_DATABASE]
        database_config = {
            "HOST": database["HOST"],
            "PORT": database["PORT"],
            "DATABASE": database["NAME"],
            "SCHEMA": self.orm_task.schema,
            "USERNAME": database["USER"],
            "PASSWORD": database["PASSWORD"],
        }

        self.starting_progress = self.orm_task.progress
        self.configure_file_logger()
        self.logger.info("Exporting shapefile...")

        qgs, processing, gdal_utils, is_windows = initQgis()

        # parse export configuration
        config = ShpExportConfig(self.ref_year)
        print(f"Exporting Shape for {len(config)} config files")
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
                    f"Skipping '{shape_conf['name']}' shapefile generation."
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
            try:
                shp_out_path = str(target_shapefile.absolute())
                sql_sources = shape_conf["sql_sources"]
                sql = sql_sources[0]
                for i in range(1, len(sql_sources)):
                    sql += " UNION ALL " + sql_sources[i]
                print(sql)
                commands = self.create_gdal_commands(database_config, shp_out_path, sql, is_windows)
                self.execute_command(gdal_utils, commands)
            except Exception as e1:
                self.logger.error("Exception: " + str(e1), e1)
                raise e1
            # update task status
            step += 1
            self.update_progress(step, total_shapes_number)

        self.set_max_progress()
