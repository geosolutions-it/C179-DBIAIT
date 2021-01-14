import copy
from django.db import connection
from app.scheduler.utils import Schema, dictfetchall


class Municipalities:

    _data = {}

    def __init__(self, schema=Schema.ANALYSIS, year=None):
        self.schema = schema
        self.table = f"{self.schema}.decod_com"
        if year:
            self.table += f"_{year}"
        self.fetch_items()

    @property
    def data(self):
        return copy.deepcopy(self._data)

    def fetch_items(self):
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT pro_com_acc, denom_com_acc, pro_com, denom_com FROM {self.table} ORDER BY pro_com"
            )
            municipalities = dictfetchall(cursor)
            for municipality in municipalities:
                self._data[municipality['pro_com']] = municipality

    def translate_code(self, pro_com) -> int:
        """
        Method translating pro_com using the decod_com table
        """
        code = int(pro_com)
        if code in self._data:
            return self._data[code]["pro_com_acc"]
        return code
