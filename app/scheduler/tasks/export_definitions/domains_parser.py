import copy
import ast
from django.db import connection
from app.scheduler.utils import Schema, dictfetchall


class Domains:

    _data = {}

    def __init__(self, schema=Schema.ANALYSIS, year=None):
        self.schema = schema
        self.table = f"{self.schema}.all_domains"
        if year:
            self.table += f"_{year}"
        self.fetch_domains()

    @property
    def data(self):
        return copy.deepcopy(self._data)

    def fetch_domains(self):
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT distinct dominio_gis FROM {self.table}"
            )
            domains = cursor.fetchall()

            for domain in domains:
                cursor.execute(
                    f"SELECT valore_gis, valore_netsic FROM {self.table} WHERE dominio_gis='{domain[0]}'"
                )
                rows = dictfetchall(cursor)
                self._data.update(
                    {
                        domain[0]: {
                            row["valore_gis"]: row["valore_netsic"] for row in rows
                        }
                    }
                )

    def translate(self, dominio_gis: str, valore_gis: str) -> str:
        """
        Method translating valore_gis of a certain dominio_gis into valore_netsic
        """
        result = self._data[dominio_gis][valore_gis]
        if result and result.replace(".", "").isdigit():
            result = ast.literal_eval(result)
        return result
