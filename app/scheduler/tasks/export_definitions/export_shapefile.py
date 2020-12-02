import os
import pathlib

from qgis.core import (
    QgsApplication,
    QgsVectorLayer,
    QgsDataSourceUri,
    QgsVectorFileWriter,
)

from django.conf import settings
from django.db import connection

from app.scheduler.models import Task
from app.scheduler.utils import translate_schema_to_db_alias


class ShapeExporter:
    qgis = None

    @staticmethod
    def initQGis():
        if ShapeExporter.qgis is None:
            ShapeExporter.qgis = QgsApplication([], False)
            QgsApplication.initQgis()

    def __init__(
        self,
        task_id: int,
        table: str,
        name: str,
        shape_file_folder: pathlib.Path,
        fields: list,
        filter_query: str,
        pre_process: str,
        year=None,
    ):
        self.year = year
        self.table = table
        self.name = name
        self.fields = fields
        self.task_id = task_id
        self.shape_file_folder = shape_file_folder
        self.filter = filter_query
        self.pre_process = pre_process

        task = Task.objects.get(id=task_id)
        self.schema = task.schema
        self.database = settings.DATABASES[translate_schema_to_db_alias(task.schema)]

    def export_preprocess(self):
        analysis_cursor = connection.cursor()
        with analysis_cursor as cursor:
            cursor.callproc(
                f"{settings.DATABASE_SCHEMAS['analysis']}.{self.pre_process}"
            )
            result = cursor.fetchone()
            print(f"Export preprocess of [name={self.name}] returned:\n{result}")

    def execute(self):
        ShapeExporter.initQGis()
        if self.pre_process:
            self.export_preprocess()

        self.shape_file_folder.mkdir(parents=True, exist_ok=True)

        uri = QgsDataSourceUri()
        uri.setConnection(
            self.database["HOST"],
            str(self.database["PORT"]),
            self.database["NAME"],
            self.database["USER"],
            self.database["PASSWORD"],
        )

        uri.setDataSource(self.schema, self.table, "geom", aSql=self.filter)

        vlayer = QgsVectorLayer(uri.uri(), self.table, "postgres")
        print("Feature count: " + str(vlayer.featureCount()))
        print("Invalid Layer: " + str(vlayer.InvalidLayer))
        filename = os.path.join(self.shape_file_folder, self.table + ".shp")
        fields = vlayer.fields()

        attrs = [
            fields.indexFromName(field["name"])
            for field in self.fields
            if fields.indexFromName(field["name"])
        ]
        result = QgsVectorFileWriter.writeAsVectorFormat(
            layer=vlayer,
            fileName=filename,
            fileEncoding="utf-8",
            driverName="ESRI Shapefile",
            attributes=attrs,
        )
        del vlayer
        print(result)
