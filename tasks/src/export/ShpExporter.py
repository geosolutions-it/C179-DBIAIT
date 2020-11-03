import os
from qgis.core import QgsVectorLayer, QgsDataSourceUri, QgsVectorFileWriter


class ShpExporter:

    def __init__(self, folder):
        self.folder = folder

    def execute(self):
        host = "127.0.0.1"
        port = "5432"
        database = "pa"
        schema = "dbiait_analysis"
        user = "postgres"
        password = "pc060574"

        tablename = "localita"
        geometrycol = "geom"

        uri = QgsDataSourceUri()
        uri.setConnection(host, port, database, user, password)
        uri.setDataSource(schema, tablename, geometrycol, "", "")
        vlayer = QgsVectorLayer(uri.uri(False), tablename, "postgres")
        print(dir(vlayer))
        filename = os.path.join(self.folder, tablename + ".shp")
        QgsVectorFileWriter.writeAsVectorFormat(layer=vlayer, fileName=filename, fileEncoding="utf-8", driverName="ESRI Shapefile")
        del vlayer


if __name__ == "__main__":
    exporter = ShpExporter("c:\\temp")
    exporter.execute()
