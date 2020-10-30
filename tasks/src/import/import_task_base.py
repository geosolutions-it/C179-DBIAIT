#!/usr/bin/python3
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import processing
from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms
from processing.core.Processing import Processing
from processing.tools import postgis

from django.conf import settings


class ImportTaskBase:

    config_file = os.path.join(os.path.dirname(__file__), "../../config/")

    def __init__(self, gpkg_path, offset=0, limit=50, schema=Schema.ANALYSIS):
        database = settings.DATABASES[settings.TASKS_DATABASE]
        self.database_config = {
            'HOST': database['HOST'],
            'PORT': database['PORT'],
            'DATABASE': database['NAME'],
            'SCHEMA': schema,
            'USERNAME': database['USER'],
            'PASSWORD': database['PASSWORD']
        }

        self.gpkg_path = gpkg_path
        self.qs_pg_prefix = "PostgreSQL/connections/" + database['NAME'] + "/"
        self.processing = None
        self.postgis = None
        self.qgs = None
        self.initQgis()
        self.context = QgsProcessingContext()
        self.offset = offset
        self.limit = limit

    def initQgis(self):
        """
        Initialize QGIS application with all necessary to work with processing tools
        """
        QgsApplication.setPrefixPath(settings.QGIS_PATH, True)
        self.qgs = QgsApplication([], True)
        self.qgs.initQgis()
        import processing
        from processing.core.Processing import Processing
        from processing.tools import postgis
        Processing.initialize()
        QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())
        self.processing = processing
        self.postgis = postgis

    def define_pg_connection(self):
        """
        Define QgsSettings to make connection available by name to the ImportToPG algorithm
        """
        qs = QgsSettings()
        qs.setValue(self.qs_pg_prefix + "allowGeometrylessTables", True)
        qs.setValue(self.qs_pg_prefix + "authcfg", "")
        qs.setValue(self.qs_pg_prefix + "database", self.database_config["DATABASE"])
        qs.setValue(self.qs_pg_prefix + "dontResolveType", False)
        qs.setValue(self.qs_pg_prefix + "estimatedMetadata", False)
        qs.setValue(self.qs_pg_prefix + "geometryColumnsOnly", False)
        qs.setValue(self.qs_pg_prefix + "host", self.database_config["HOST"])
        qs.setValue(self.qs_pg_prefix + "port", self.database_config["PORT"])
        qs.setValue(self.qs_pg_prefix + "username", self.database_config["USERNAME"])
        qs.setValue(self.qs_pg_prefix + "password", self.database_config["PASSWORD"])
        qs.setValue(self.qs_pg_prefix + "projectsInDatabase", False)
        qs.setValue(self.qs_pg_prefix + "publicOnly", False)
        qs.setValue(self.qs_pg_prefix + "savePassword", True)
        qs.setValue(self.qs_pg_prefix + "saveUsername", True)
        qs.setValue(self.qs_pg_prefix + "service", "")
        qs.setValue(self.qs_pg_prefix + "sslmode", "SslDisable")
