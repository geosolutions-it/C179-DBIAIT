import json
import os

from django.conf import settings
from app.scheduler.utils import Schema
from shutil import copy
import ntpath


class BaseFreezeDefinition:
    def __init__(self, schema=Schema.FREEZE):
        database = settings.DATABASES[settings.TASKS_DATABASE]
        self.database_config = {
            "HOST": database["HOST"],
            "PORT": database["PORT"],
            "DATABASE": database["NAME"],
            "SCHEMA": schema,
            "USERNAME": database["USER"],
            "PASSWORD": database["PASSWORD"],
        }
        self.config_filename = settings.EXPORT_CONF_FILE
        self.shp_file = settings.SHAPEFILE_EXPORT_CONFIG
        self.netsic_file = settings.EXPORT_XLS_SEED_FILE
        self.export_folder = f"{settings.EXPORT_FOLDER}/freeze/"

    def _year_config_file_exists(self, ref_year):
        """
        Check if configurations files are available, will raise a FileNotFound exception
        """
        conf_file = os.path.exists(self.config_filename.substitute(mapping={'year': ref_year}))
        shp_file = os.path.exists(self.shp_file.substitute(mapping={'year': ref_year}))
        netsic_file = os.path.exists(self.netsic_file.substitute(mapping={'year': ref_year}))
        if not all([conf_file, shp_file, netsic_file]):
            return True
        raise FileExistsError("I file di configurazione per l'anno selezionato sono già stati storicizzati. "
                              "Prima di riprovare, è necessario eliminare i file esistenti")

    def _create_year_folder(self, ref_year):
        try:
            export_folder = f"{settings.EXPORT_FOLDER}/config/{ref_year}"
            if not os.path.exists(export_folder):
                os.makedirs(export_folder, exist_ok=True)

            conf_filename = ntpath.basename(self.config_filename.substitute())
            shp_file = ntpath.basename(self.shp_file.substitute())
            netsic_file = ntpath.basename(self.netsic_file.substitute())

            copy(self.config_filename.substitute(), f"{export_folder}/{conf_filename}")
            copy(self.shp_file.substitute(), f"{export_folder}/{shp_file}")
            copy(self.netsic_file.substitute(), f"{export_folder}/{netsic_file}")
            return True
        except Exception as e:
            raise e

    def _handle_sheet_files(self, ref_year):
        output_path = f"{settings.EXPORT_FOLDER}/config/{ref_year}/sheet_configs/"
        input_dir = f"{settings.EXPORT_CONF_DIR}/current/sheet_configs/"
        return self._add_year_to_table_name(output_path, input_dir, ref_year)

    def _handle_shp_files(self, ref_year):
        output_path = f"{settings.EXPORT_FOLDER}/config/{ref_year}/shapefile_configs/"
        input_dir = f"{settings.EXPORT_CONF_DIR}/current/shapefile_configs/"
        return self._add_year_to_table_name(output_path, input_dir, ref_year)

    @staticmethod
    def _add_year_to_table_name(output_dir, input_dir, ref_year):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        for config in os.listdir(input_dir):
            with open(f"{input_dir}/{config}", 'r') as file:
                sheet_conf = json.loads(file.read())

            for x in sheet_conf['sources']:
                table_name = x.get('table')['name']
                table = {"name": f"{table_name}_{ref_year}", "alias": table_name}
                x['table'] = table
                join_tables = x.get('join')
                if join_tables is not None:
                    for y in join_tables:
                        join_table_name = y.get('table')['name']
                        j_table = {"name": f"{join_table_name}_{ref_year}", "alias": join_table_name}
                        y['table'] = j_table

            with open(f"{output_dir}/{config}", 'w') as new_conf:
                new_conf.write(json.dumps(sheet_conf, indent=4))
        return True
