import json
import pathlib
import traceback

# import QGis API
from qgis.core import *

from django.conf import settings
from app.scheduler.utils import Schema, TaskStatus
from django.utils import timezone
from app.scheduler.exceptions import SchedulerException
from app.scheduler.models import Task, ImportedLayer
from .base_import import BaseImportDefinition


class GpkgImportDefinition(BaseImportDefinition):
    def __init__(
        self,
        gpkg_path,
        orm_task: Task,
        offset=0,
        limit=50,
        schema=Schema.ANALYSIS,
        qgs=None,
        processing=None,
        postgis=None,
    ):
        super().__init__(schema=schema)

        self.gpkg_path = gpkg_path
        self.orm_task = orm_task
        self.offset = offset
        self.limit = limit

        self.qgs = qgs
        self.processing = processing
        self.postgis = postgis

    @staticmethod
    def get_feature_classes():
        """
        Read list of Feature Classes to import from the configuration file (import.json)
        """
        config_file = pathlib.Path(settings.IMPORT_CONF_FILE)
        if not config_file.exists():
            raise SchedulerException(
                f"Import configuration file {settings.IMPORT_CONF_FILE} does not exist."
            )

        with open(config_file, "r") as cfg:
            config = json.load(cfg)
        fc_list = config["featureclasses"]
        fc_list.sort()
        return fc_list

    def _get_gpkg_vector_layer(self, name):
        """
        Return a QgsVectorLayer by name
        """
        layer = None
        try:
            gpkg_layer_name = self.gpkg_path + "|layername=" + name
            vlayer = QgsVectorLayer(gpkg_layer_name, name, "ogr")
            if vlayer.isValid():
                layer = vlayer
        except Exception as e:
            print(e)
        return layer

    def get_gpkg_vector_layer(self, name):
        """
        Return a QgsVectorLayer by name (trying also the name in upper/lower case)
        """
        layer = self._get_gpkg_vector_layer(name)
        if layer is None:
            layer = self._get_gpkg_vector_layer(name.lower())
        if layer is None:
            layer = self._get_gpkg_vector_layer(name.upper())
        return layer

    def get_gtype(self, layer):
        """
        Return the geometry type of the layer decoded for the processing tool
        """
        gtype = layer.geometryType()
        if gtype == 0:
            # POINT
            return 3
        elif gtype == 1:
            # LINESTRING
            return 9
        elif gtype == 2:
            # LINESTRING
            return 8
        return None

    def import_into_postgis(self, name, cont, feedback):
        """
        Run the importintopostgis algorithm
        """
        result = None
        vlayer = self.get_gpkg_vector_layer(name)
        if vlayer is None:
            raise AttributeError("vlayer is None")
        try:
            vlayer = self.get_gpkg_vector_layer(name)
            print(
                "importing layer ("
                + str(cont)
                + "): "
                + name
                + " => "
                + str(vlayer.featureCount())
            )
            # Export in PostgreSQL
            gtype = self.get_gtype(vlayer)
            alg_params = {
                'ADDFIELDS': False,
                'APPEND': False,
                'A_SRS': None,
                'CLIP': False,
                'DATABASE': self.database_config["DATABASE"],
                'DIM': 0,
                'GEOCOLUMN': 'geom',
                'GT': '',
                'GTYPE': gtype,
                'INDEX': False,
                'INPUT': vlayer,
                'LAUNDER': False,
                'OPTIONS': '',
                'OVERWRITE': True,
                'PK': '',
                'PRECISION': True,
                'PRIMARY_KEY': '',
                'PROMOTETOMULTI': True,
                'SCHEMA': self.database_config["SCHEMA"],
                'SEGMENTIZE': '',
                'SHAPE_ENCODING': '',
                'SIMPLIFY': '',
                'SKIPFAILURES': False,
                'SPAT': None,
                'S_SRS': None,
                'TABLE': name,
                'T_SRS': None,
                'WHERE': ''
            }
            result = self.processing.run('gdal:importvectorintopostgisdatabaseavailableconnections', alg_params,
                                         context=self.context, feedback=feedback, is_child_algorithm=True)
        except Exception as e:
            print(e)
            traceback.print_exc()
        return result

    def run(self):
        """
        Start the import task
        """
        if self.gpkg_path is None:
            raise FileNotFoundError
        if self.database_config is None:
            raise AttributeError

        self.define_pg_connection()
        to_load = self.get_feature_classes()
        cont = self.offset
        n_step = len(to_load)
        fbk = QgsProcessingFeedback()
        feedback = QgsProcessingMultiStepFeedback(self.limit, fbk)
        if n_step > 0:
            prg_step = 100.0 / n_step
            for layername in to_load[self.offset: self.offset + self.limit]:
                cont += 1
                print(layername + ": " + str(cont))
                start_date = timezone.now()
                end_date = None
                task_status = TaskStatus.RUNNING
                try:
                    self.import_into_postgis(layername.lower(), cont, feedback)
                    task_status = TaskStatus.SUCCESS
                    end_date = timezone.now()
                except Exception as e:
                    print(layername + ": " + str(e))
                    task_status = TaskStatus.FAILED
                finally:
                    ImportedLayer.objects.create(
                        task=self.orm_task,
                        layer_name=layername.lower(),
                        import_start_timestamp=start_date,
                        import_end_timestamp=end_date,
                        status=task_status
                    )
                prg = cont / n_step
                feedback.setCurrentStep(cont - self.offset)
                if feedback.isCanceled():
                    break
