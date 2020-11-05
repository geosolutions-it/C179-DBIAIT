import os
from qgis.core import QgsApplication, QgsVectorLayer, QgsDataSourceUri, QgsVectorFileWriter
from django.conf import settings

qgs = None
database = settings.DATABASES["default"]


class ShapeExporter:
    qgis = None

    @staticmethod
    def initQGis():
        if ShapeExporter.qgis is None:
            ShapeExporter.qgis = QgsApplication([], False)
            QgsApplication.initQgis()

    def __init__(self, task_id: int, table: str, shape_file: str, shape_file_folder: str, fields: list, filter: str, year=None):
        self.folder = settings.TEMP_EXPORT_DIR
        self.year = year
        self.table = table
        self.shape_file = shape_file
        self.fields = fields
        self.task_id = task_id
        self.shape_file_folder = shape_file_folder
        self.filter = filter

    def execute(self):
        ShapeExporter.initQGis()

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

        geometrycol = u"geom"

        uri = QgsDataSourceUri()
        uri.setConnection(database["HOST"], str(database["PORT"]), database["NAME"], database["USER"], database["PASSWORD"])
        uri.setDataSource(schema, self.table, geometrycol, aSql=self.filter)

        vlayer = QgsVectorLayer(uri.uri(), self.table, "postgres")
        print("Feature count: " + str(vlayer.featureCount()))
        print("Invalid Layer: " + str(vlayer.InvalidLayer))
        filename = os.path.join(shapefile_folder, self.table + ".shp")
        fields = vlayer.fields()

        attrs = [fields.indexFromName(field["name"]) for field in self.fields if fields.indexFromName(field["name"])]
        result = QgsVectorFileWriter.writeAsVectorFormat(layer=vlayer, fileName=filename, fileEncoding="utf-8", driverName="ESRI Shapefile", attributes=attrs)
        del vlayer
        print(result)
