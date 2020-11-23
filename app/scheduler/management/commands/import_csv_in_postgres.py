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
        db_cursor, db_conn = self.__create_db_connection()
        file_available = os.listdir(options["folder_path"][0])
        for csv_file in file_available:
            try:
                with open(f'{options["folder_path"][0]}\\{csv_file}') as f:
                    next(f)  # needed for skip the headers
                    db_cursor.copy_from(file=f, table=f"dbiait_analysis.{csv_file.lower().replace('.csv', '')}", sep=";")
                    db_conn.commit()
            except Exception as e:
                self.stdout.write(f"Error during CSV upload for file {csv_file}, Log: {e}")
            finally:
                db_cursor.close()
                db_conn.close()

            self.stdout.write("File successfully loaded")

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
