from django.conf import settings

from app.scheduler.models import AllDomains
from .base_import import BaseImportDefinition


class CsvImportDefinition(BaseImportDefinition):
    @staticmethod
    def run():
        """
        Run the CSV import process
        """
        # remove everything from all_domains
        AllDomains.objects.all().delete()

        # upload new data to all_domains
        inserted_row_count = AllDomains.objects.from_csv(
            settings.IMPORT_DOMAINS_FILE,
            {
                "dominio_netsic": "Dominio NetSic",
                "dominio_gis": "Dominio GIS",
                "valore_gis": "Valore GIS",
                "descrizione_gis": "Descrizione GIS",
                "valore_netsic": "Valore NETSIC",
            },
        )
        print(f"Processed {inserted_row_count} lines.")
