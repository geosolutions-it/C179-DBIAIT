#!/usr/bin/python3
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import traceback
import json
from base_task import BaseTask
from qgis.core import *
from qgis.analysis import QgsNativeAlgorithms

# list available algorithms
#for alg in QgsApplication.processingRegistry().algorithms():
#    print(alg.id(), "->", alg.displayName())


class ImportTaskBase(BaseTask):

    def __init__(self, config=None, offset=0, limit=50):
        BaseTask.__init__(self, config)
        self.gpkg_path = self.get_parameter("GPKG_PATH")
        self.qgis_path = self.get_parameter("QGIS_PATH")
        self.database = self.get_parameter("DATABASE")
        self.qs_pg_prefix = "PostgreSQL/connections/" + ImportTaskBase.DB_CONNECTION_NAME() + "/"
        self.processing = None
        self.postgis = None
        self.qgs = None
        self.initQgis()
        self.context = QgsProcessingContext()
        self.offset = offset
        self.limit = limit

    #@staticmethod
    def DB_CONNECTION_NAME():
        return 'DBIAIT'

    def initQgis(self):
        """
        Initialize QGIS application with all necessary to work with processing tools
        """
        qgis_path = self.qgis_path
        QgsApplication.setPrefixPath(qgis_path, True)
        self.qgs = QgsApplication([], True)
        self.qgs.initQgis()
        sys.path.append(qgis_path + r'\python\plugins')
        #global processing
        import processing
        from processing.core.Processing import Processing
        from processing.tools import postgis
        Processing.initialize()
        QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())
        self.processing = processing
        self.postgis = postgis

    @staticmethod
    def get_config_folder():
        """
        Return path to the task's config folder
        """
        dir_name = os.path.dirname(__file__)
        return os.path.join(dir_name, "../../config/")

    def define_pg_connection(self):
        """
        Define QgsSettings to make connection available by name to the ImportToPG algorithm
        """
        qs = QgsSettings()
        qs.setValue(self.qs_pg_prefix + "allowGeometrylessTables", True)
        qs.setValue(self.qs_pg_prefix + "authcfg", "")
        qs.setValue(self.qs_pg_prefix + "database", self.database["DATABASE"])
        qs.setValue(self.qs_pg_prefix + "dontResolveType", False)
        qs.setValue(self.qs_pg_prefix + "estimatedMetadata", False)
        qs.setValue(self.qs_pg_prefix + "geometryColumnsOnly", False)
        qs.setValue(self.qs_pg_prefix + "host", self.database["HOST"])
        qs.setValue(self.qs_pg_prefix + "port", self.database["PORT"])
        qs.setValue(self.qs_pg_prefix + "username", self.database["USERNAME"])
        qs.setValue(self.qs_pg_prefix + "password", self.database["PASSWORD"])
        qs.setValue(self.qs_pg_prefix + "projectsInDatabase", False)
        qs.setValue(self.qs_pg_prefix + "publicOnly", False)
        qs.setValue(self.qs_pg_prefix + "savePassword", True)
        qs.setValue(self.qs_pg_prefix + "saveUsername", True)
        qs.setValue(self.qs_pg_prefix + "service", "")
        qs.setValue(self.qs_pg_prefix + "sslmode", "SslDisable")