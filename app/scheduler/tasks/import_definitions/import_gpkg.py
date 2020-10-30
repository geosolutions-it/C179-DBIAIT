import json
import pathlib
import traceback

# import QGis API
from qgis.core import *

from django.conf import settings

from app.scheduler.utils import Schema
from app.scheduler.exceptions import SchedulerException
from .base_import import BaseImportDefinition


class GpkgImportDefinition(BaseImportDefinition):

    def __init__(self, gpkg_path, offset=0, limit=50, schema=Schema.ANALYSIS):
        super().__init__(schema=schema)

        self.gpkg_path = gpkg_path
        self.offset = offset
        self.limit = limit

    @staticmethod
    def get_feature_classes():
        """
        Read list of Feature Classes to import from the configuration file (import.json)
        """
        config_file = pathlib.Path(settings.IMPORT_CONF_FILE)
        if not config_file.exists():
            raise SchedulerException(f'Import configuration file {settings.IMPORT_CONF_FILE} does not exist.')

        with open(config_file, 'r') as cfg:
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

    def import_into_postgis(self, name, cont, feedback):
        """
        Run the importintopostgis algorithm
        """
        result = None
        try:
            vlayer = self.get_gpkg_vector_layer(name)
            print("importing layer (" + str(cont) + "): " + name + " => " + str(vlayer.featureCount()))
            # Export in PostgreSQL
            alg_params = {
                'CREATEINDEX': True,
                'DATABASE': self.database_config['DATABASE'],
                'DROP_STRING_LENGTH': False,
                'ENCODING': 'UTF-8',
                'FORCE_SINGLEPART': False,
                'GEOMETRY_COLUMN': 'geom',
                'INPUT': vlayer,
                'LOWERCASE_NAMES': True,
                'OVERWRITE': True,
                'PRIMARY_KEY': None,
                'SCHEMA': self.database_config['SCHEMA'],
                'TABLENAME': name
            }
            result = self.processing.run('qgis:importintopostgis', alg_params, context=self.context, feedback=feedback, is_child_algorithm=True)
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
            prg_step = 100.0/n_step
            for layername in to_load[self.offset:self.offset+self.limit]:
                cont += 1
                print(layername + ": " + str(cont))
                self.import_into_postgis(layername.lower(), cont, feedback)
                prg = cont/n_step
                feedback.setCurrentStep(cont-self.offset)
                if feedback.isCanceled():
                    break
