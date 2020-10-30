import csv
import pathlib

# import QGis API
from qgis.core import *

from django.conf import settings

from app.scheduler.utils import Schema
from app.scheduler.exceptions import SchedulerException
from .base_import import BaseImportDefinition


class CsvImportDefinition(BaseImportDefinition):

    def __init__(self, schema=Schema.ANALYSIS):
        super().__init__(schema=schema)
        self.columns = {
            "DOMINIO_GIS": 0,
            "VALORE_GIS": 1,
            "DESCRIZIONE_GIS": 2,
            "DOMINIO_NETSIC": 3,
            "VALORE_NETSIC": 4
        }
        self.domains = {}
        self.db = None

    def check_columns(self, row):
        if len(row) == 5:
            row = [i.upper().replace(" ", "_") for i in row]
            for key in self.columns.keys():
                try:
                    index = row.index(key)
                    self.columns[key] = index
                except Exception as e:
                    return False
        else:
            return False

        return True

    def drop_table_if_exists(self, table):
        try:
            self.db.delete_table(table, self.database_config["SCHEMA"])
        except Exception as e:
            pass

    def quote_value(self, value):
        return "'" + value.replace("'", "''") + "'"

    def row_to_values(self, row):
        """
        Transform row into a tuple of elements in the correct order
        """
        values = (
            self.quote_value(row[self.columns['DOMINIO_GIS']]),
            self.quote_value(row[self.columns['VALORE_GIS']]),
            self.quote_value(row[self.columns['DESCRIZIONE_GIS']]),
            self.quote_value(row[self.columns['DOMINIO_NETSIC']]),
            self.quote_value(row[self.columns['VALORE_NETSIC']]),
        )
        return values

    def run(self):
        """
        Run the CSV import process
        """
        self.define_pg_connection()
        csv_path = pathlib.Path(settings.IMPORT_DOMAINS_FILE)
        if not csv_path.exists():
            raise SchedulerException(f'Import configuration file {settings.IMPORT_DOMAINS_FILE} does not exist.')

        with open(csv_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    if self.check_columns(row) is False:
                        raise Exception("Invalid fields: " + ", ".join(row))
                    line_count += 1
                else:
                    table_name = row[self.columns["DOMINIO_GIS"]]
                    if table_name not in self.domains.keys():
                        self.domains[table_name] = []
                    self.domains[table_name].append(row)
                    line_count += 1
            print(f'Processed {line_count} lines.')

        self.db = self.postgis.GeoDB.from_name(self.database_config['DATABASE'])
        self.db.empty_table('all_domains', self.database_config["SCHEMA"])
        crs = self.db.con.cursor()

        try:
            # For each table
            for table in self.domains.keys():
                for row in self.domains[table]:
                    values = self.row_to_values(row)
                    self.db.insert_table_row('all_domains', values, self.database_config["SCHEMA"], cursor=crs)
            self.db.con.commit()
        except Exception as e:
            self.db.con.rollback()
