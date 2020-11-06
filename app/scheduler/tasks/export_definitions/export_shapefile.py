import os
from qgis.core import QgsApplication, QgsVectorLayer, QgsDataSourceUri, QgsVectorFileWriter
from django.conf import settings
from django.db import connection

qgs = None
database = settings.DATABASES["default"]


class ShapeExporter:
    qgis = None

    @staticmethod
    def initQGis():
        if ShapeExporter.qgis is None:
            ShapeExporter.qgis = QgsApplication([], False)
            QgsApplication.initQgis()

    def __init__(self, task_id: int, table: str, name: str, shape_file_folder: str, fields: list, filter_query: str, pre_process: str, year=None):
        self.folder = settings.TEMP_EXPORT_DIR
        self.year = year
        self.table = table
        self.name = name
        self.fields = fields
        self.task_id = task_id
        self.shape_file_folder = shape_file_folder
        self.filter = filter_query
        self.pre_process = pre_process

    def export_preprocess(self):
        analysis_cursor = connection.cursor()
        with analysis_cursor as cursor:
            cursor.callproc(
                f"{settings.DATABASE_SCHEMAS[u'analysis']}.{self.pre_process}")
            result = cursor.fetchone()
            print(f"Export preprocess of [name={self.name}] returned:\n{result}")

    def execute(self):
        ShapeExporter.initQGis()
        if self.pre_process:
            self.export_preprocess()

        if os.path.exists(settings.TEMP_EXPORT_DIR):
            shapefile_folder = os.path.join(settings.TEMP_EXPORT_DIR, str(self.task_id), self.shape_file_folder)
            not os.path.exists(shapefile_folder) and os.makedirs(shapefile_folder)
        else:
            raise Exception(u"No temporaly export folder configuared")

        schema = settings.DATABASE_SCHEMAS[u"analysis"]
        if self.year is not None:
            schema = settings.DATABASE_SCHEMAS[u"freeze"]

        if self.year is not None:
            f"{self.table}_{str(self.year)}"

        uri = QgsDataSourceUri()
        uri.setConnection(database[u"HOST"], str(database[u"PORT"]), database[u"NAME"], database[u"USER"], database[u"PASSWORD"])
        filter_condition = f" WHERE {self.filter}" if self.filter else u""
        print(f"FILTER CONDITION: SELECT * FROM {schema}.{self.table}{filter_condition};")
        uri.setDataSource(schema, self.table, u"geom", aSql=f"SELECT * FROM {schema}.{self.table}{filter_condition};")

        vlayer = QgsVectorLayer(uri.uri(), self.table, "postgres")
        print("Feature count: " + str(vlayer.featureCount()))
        print("Invalid Layer: " + str(vlayer.InvalidLayer))
        filename = os.path.join(shapefile_folder, self.table + ".shp")
        fields = vlayer.fields()

        attrs = [fields.indexFromName(field[u"name"]) for field in self.fields if fields.indexFromName(field[u"name"])]
        result = QgsVectorFileWriter.writeAsVectorFormat(layer=vlayer, fileName=filename, fileEncoding=u"utf-8", driverName=u"ESRI Shapefile", attributes=attrs)
        del vlayer
        print(result)
