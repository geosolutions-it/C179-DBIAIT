# import QGis API
from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms

from django.conf import settings
from app.scheduler.utils import Schema


class BaseImportDefinition:

    def __init__(self, schema=Schema.ANALYSIS):
        database = settings.DATABASES[settings.TASKS_DATABASE]
        self.database_config = {
            'HOST': database['HOST'],
            'PORT': database['PORT'],
            'DATABASE': database['NAME'],
            'SCHEMA': schema,
            'USERNAME': database['USER'],
            'PASSWORD': database['PASSWORD']
        }
        self.qs_pg_prefix = "PostgreSQL/connections/" + database['NAME'] + "/"
        self.processing = None
        self.postgis = None
        self.qgs = None
        self.initQgis()
        self.context = QgsProcessingContext()

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
