import copy
from django.db import connection
from app.scheduler.utils import Schema, dictfetchall


class Domains:

    _data = {}

    def __init__(self, schema=Schema.ANALYSIS):
        self.schema = schema
        self.fetch_domains()

    @property
    def data(self):
        return copy.deepcopy(self._data)

    def fetch_domains(self):
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT distinct dominio_gis FROM {self.schema}.all_domains"
            )
            domains = cursor.fetchall()

            for domain in domains:
                cursor.execute(
                    f"SELECT valore_gis, valore_netsic FROM {self.schema}.all_domains WHERE dominio_gis='{domain[0]}'"
                )
                rows = dictfetchall(cursor)
                self._data.update(
                    {
                        domain[0]: {
                            row['valore_gis']: row['valore_netsic']
                            for row in rows
                        }
                    }
                )

    def translate(self, dominio_gis: str, valore_gis: str) -> str:
        """
        Method translating valore_gis of a certain dominio_gis into valore_netsic
        """
        return self._data[dominio_gis][valore_gis]
