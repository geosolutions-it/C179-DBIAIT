import os, sys
import csv
from import_task_base import ImportTaskBase
from qgis.core import *
from qgis.PyQt.QtCore import QVariant


class ImportCsvTask(ImportTaskBase):

    def __init__(self, config=None):
        ImportTaskBase.__init__(self, config)
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
        ret = True
        if len(row) == 5:
            row = [i.upper().replace(" ", "_") for i in row]
            for key in self.columns.keys():
                try:
                    index = row.index(key)
                    self.columns[key] = index
                except Exception as e:
                    ret = False
        else:
            ret=False
        return ret

    def drop_table_if_exists(self, table):
        try:
            self.db.delete_table(table, self.database["SCHEMA"])
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
        csv_path = os.path.join(ImportTaskBase.get_config_folder(), 'domains.csv')
        with open(csv_path) as csv_file:
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

        self.db = self.postgis.GeoDB.from_name(ImportTaskBase.DB_CONNECTION_NAME())
        self.db.empty_table('all_domains', self.database["SCHEMA"])
        crs = self.db.con.cursor()
        try:
            # For each table
            for table in self.domains.keys():
                for row in self.domains[table]:
                    values = self.row_to_values(row)
                    self.db.insert_table_row('all_domains', values, self.database["SCHEMA"], cursor=crs)
            self.db.con.commit()
        except Exception as e:
            self.db.con.rollback()


if __name__ == "__main__":
    task = ImportCsvTask(config={
        'QGIS_PATH': r'C:\OSGeo4W64\apps\qgis',
        'GPKG_PATH': r'C:\geo-solutions\repositories\C179-PUBLIACQUA\NETSIC\GPKG\PBAP_20201005_000734.gpkg',
        'DATABASE': {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DATABASE': 'pa',
            'SCHEMA': 'DBIAIT_ANALYSIS',
            'USERNAME': 'postgres',
            'PASSWORD': 'pc060574'
        }
    })
    task.run()
