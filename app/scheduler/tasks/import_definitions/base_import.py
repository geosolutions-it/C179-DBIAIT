# import QGis API
import sys
import os
from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms

from django.conf import settings
from app.scheduler.utils import Schema


class BaseImportDefinition:
    def __init__(self, schema=Schema.ANALYSIS):
        database = settings.DATABASES[settings.TASKS_DATABASE]
        self.database_config = {
            "HOST": database["HOST"],
            "PORT": database["PORT"],
            "DATABASE": database["NAME"],
            "SCHEMA": schema,
            "USERNAME": database["USER"],
            "PASSWORD": database["PASSWORD"],
        }
        self.qs_pg_prefix = "PostgreSQL/connections/" + database["NAME"] + "/"
        self.context = QgsProcessingContext()

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


def initQgis():
    """
    Method initializing QGIS application with all necessary to work with processing tools

    Note: if initialization is called more than once in a single Django/Dramatiq handler, the application may
    crash with python's fatal error: Segmentation fault, so make sure to reuse the QGis app between your classes.

    :returns: (qgs, processing, postgis)
    """
    os.environ["QT_QPA_PLATFORM"] = "offscreen"
    QgsApplication.setPrefixPath(settings.QGIS_PATH, True)
    """
    provider_registry = QgsProviderRegistry.instance()
    res = True
    if provider_registry.providerMetadata('postgres') is None:
        metadata = QgsProviderMetadata("postgres", "postgres")
        res = provider_registry.registerProvider(metadata)
    print("QgsProviderMetadata(postgres), loaded => " + str(res))
    """
    qgs = QgsApplication([], False)
    qgs.initQgis()

    sys.path.append('C:\\OSGeo4W64\\apps\\qgis\\python\\plugins')

    import processing
    from processing.core.Processing import Processing
    from processing.algs.gdal.GdalUtils import GdalUtils
    from processing.tools.system import isWindows

    Processing.initialize()
    QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())
    processing = processing

    return qgs, processing, GdalUtils, isWindows
