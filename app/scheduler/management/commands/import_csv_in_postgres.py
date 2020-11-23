import os

import psycopg2
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Import csv into postgres"
    configuration_dict = {}

    def add_arguments(self, parser):
        parser.add_argument(
            "folder_path",
            nargs="+",
            type=str,
            help="Should be the absolute path of the folder with the CSVs"
        )

    def handle(self, *args, **options):
        file_available = os.listdir(options["folder_path"][0])
        for csv_file in file_available:
            db_cursor, db_conn = self.__create_db_connection()
            try:
                with open(f'{options["folder_path"][0]}\\{csv_file}') as f:
                    next(f)  # needed for skip the headers
                    table_name = f"dbiait_analysis.{csv_file.lower().replace('.csv', '')}"
                    db_cursor.execute(f"TRUNCATE table {table_name};")
                    db_cursor.copy_from(file=f, table=table_name, sep=";", null="")
                    self.stdout.write(f"Import {table_name} DONE")

            except Exception as e:
                self.stdout.write(f"ERROR: during CSV upload for file {csv_file}, Log: {e}")
            finally:
                db_conn.commit()
                db_cursor.close()
                db_conn.close()

        self.stdout.write("Import completed, please check the console for errors")

    @staticmethod
    def __create_db_connection():
        t_host = "localhost"  # either "localhost", a domain name, or an IP address.
        t_port = "5432"  # default postgres port
        t_dbname = "postgres"  # database name
        t_user = "postgres"  # database access
        t_pw = "password"
        db_conn = psycopg2.connect(
            host=t_host,
            port=t_port,
            dbname=t_dbname,
            user=t_user,
            password=t_pw
        )
        db_cursor = db_conn.cursor()
        return db_cursor, db_conn
