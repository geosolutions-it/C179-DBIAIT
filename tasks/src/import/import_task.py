#!/usr/bin/python3
import os
import sys
#sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import traceback
import json
from import_task_base import ImportTaskBase
from qgis.core import *


class ImportTask(ImportTaskBase):

    def __init__(self, config=None, offset=0, limit=50):
        ImportTaskBase.__init__(self, config, offset, limit)

    @staticmethod
    def get_feature_classes():
        """
        Read list of Feature Classes to import from the configuration file (import.json)
        """
        #dirname = os.path.dirname(__file__)
        #config_file = os.path.join(dirname, "../../config/import.json")
        config_file = os.path.join(ImportTaskBase.get_config_folder(), 'import.json')
        with open(config_file, 'r') as cfg:
            config = json.load(cfg)
        fc_list = config["featureclasses"]
        fc_list.sort()
        return fc_list

    def get_gpkg_layer_names(self):
        """
        Read and return the layers' name in the GeoPackage
        """
        if self.gpkg_path is not None:
            layer = QgsVectorLayer(self.gpkg_path, "gpkg", "ogr")
            subLayers = layer.dataProvider().subLayers()
            names = []
            count = 0
            for subLayer in subLayers:
                name = subLayer.split('!!::!!')[1]
                #count += 1
                #gpkg_layer = gpkg_path + "|layername=" + name
                #vlayer = QgsVectorLayer(gpkg_layer, name, "ogr")
                #valid = "-"
                #if vlayer.isValid():
                #    valid = "+ " + str(vlayer.geometryType())
                #print("[" + str(count) + "]: (" + valid + ") => " + name)
                names.append(name)
            return names
        else:
            return None

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
                'DATABASE': ImportTaskBase.DB_CONNECTION_NAME(),
                'DROP_STRING_LENGTH': False,
                'ENCODING': 'UTF-8',
                'FORCE_SINGLEPART': False,
                'GEOMETRY_COLUMN': 'geom',
                'INPUT': vlayer,
                'LOWERCASE_NAMES': True,
                'OVERWRITE': True,
                'PRIMARY_KEY': None,
                'SCHEMA': self.database['SCHEMA'],
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
        if self.database is None:
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


if __name__ == "__main__":
    offset = int(sys.argv[1])
    limit = int(sys.argv[2])
    task = ImportTask(config={
        'QGIS_PATH': r'C:\OSGeo4W64\apps\qgis',
        'GPKG_PATH': r'C:\geo-solutions\repositories\C179-PUBLIACQUA\NETSIC\GPKG\PBAP_20200203.gpkg',
        'DATABASE': {
            'HOST': '127.0.0.1',
            'PORT': '5432',
            'DATABASE': 'pa',
            'SCHEMA': 'dbiait_analysis',
            'USERNAME': 'postgres',
            'PASSWORD': 'pc060574'
        }
    }, offset=offset, limit=limit)
    task.run()
