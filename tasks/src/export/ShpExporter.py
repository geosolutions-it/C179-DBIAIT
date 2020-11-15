import os
from qgis.core import QgsApplication, QgsVectorLayer, QgsDataSourceUri, QgsVectorFileWriter

qgs = None


class ShpExporter:
    qgis = None

    @staticmethod
    def initQGis():
        if ShpExporter.qgis is None:
            ShpExporter.qgis = QgsApplication([], False)
            QgsApplication.initQgis()
            import sys
            sys.path.append('C:\\OSGeo4W64\\apps\\qgis\\python\\plugins')
            import processing
            from processing.core.Processing import Processing


    def __init__(self, folder, year=None):
        self.folder = folder
        self.year = year

    def execute(self):
        ShpExporter.initQGis()

        host = "localhost"
        port = "5432"
        database = "pa"
        schema = "dbiait_analysis"
        if self.year is not None:
            schema = "dbiait_freeze"

        user = "postgres"
        password = "pc060574"

        tablename = "a_acq_condotta"
        if self.year is not None:
            tablename += "_" + str(self.year)

        geometrycol = "geom"

        uri = QgsDataSourceUri()
        uri.setConnection(host, port, database, user, password)
        #uri.setUsername(user)
        #uri.setPassword(password)
        uri.setDataSource(schema, tablename, geometrycol)
        print(str(uri.uri(True)))

        #conn = "dbname='pa' host=127.0.0.1 port=5432 user='postgres' password='pc060574' sslmode=disable key='objectid' srid=25832 type=Point checkPrimaryKeyUnicity='1' table=\"dbiait_analysis\".\"a_acq_accumulo\" (geom)"
        vlayer = QgsVectorLayer(uri.uri(), tablename, "postgres")
        print("Feature count: " + str(vlayer.featureCount()))
        print("Invalid Layer: " + str(vlayer.InvalidLayer))
        filename = os.path.join(self.folder, tablename + ".shp")
        fields = vlayer.fields()
        attrs = []
        idx1 = fields.indexFromName("idgis")
        if idx1 >= 0:
            attrs.append(idx1)
        idx2 = fields.indexFromName("d_gestore")
        if idx2 >= 0:
            attrs.append(idx2)
        QgsVectorFileWriter.writeAsVectorFormat(layer=vlayer, fileName=filename, fileEncoding="utf-8", driverName="ESRI Shapefile", attributes=attrs)
        del vlayer


if __name__ == "__main__":
    exporter = ShpExporter("c:\\temp\\aaa")
    exporter.execute()
