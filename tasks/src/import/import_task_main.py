import os
import math
import subprocess
import pathlib
from import_task import ImportTask
from import_csv import ImportCsvTask

MAX_TABLES_PER_RUN = 50
path = str(pathlib.Path().absolute())
DIR_NAME = os.path.dirname(__file__)


def import_gpkg():
    fc = ImportTask.get_feature_classes()
    tot_fc = len(fc)
    steps = math.ceil(tot_fc/MAX_TABLES_PER_RUN)
    for i in range(steps):
        offset = i*MAX_TABLES_PER_RUN
        limit = MAX_TABLES_PER_RUN
        #Import of Feature Classes
        subprocess.call("python3 " + DIR_NAME + "/import_task.py " + str(offset) + " " + str(limit), shell=False)
    #Import of Domains
    subprocess.call("python3 " + DIR_NAME + "/import_csv.py", shell=False)


DATABASE = {
                'HOST': '127.0.0.1',
                'PORT': '5432',
                'DATABASE': 'dbiait',
                'SCHEMA': 'dbiait_analysis',
                'USERNAME': 'biegan',
                'PASSWORD': 'rev'
            }

def import_gpkg_2():
    fc = ImportTask.get_feature_classes()
    tot_fc = len(fc)
    steps = math.ceil(tot_fc / MAX_TABLES_PER_RUN)
    for i in range(steps):
        offset = i * MAX_TABLES_PER_RUN
        limit = MAX_TABLES_PER_RUN
        # Import of Feature Classes
        task = ImportTask(config={
            'QGIS_PATH': r'/home/biegan/Apps/qgis',
            'GPKG_PATH': r'/home/biegan/PycharmProjects/C179-DBIAIT/tasks/tests/data/PBAP_20200203_test.gpkg',
            'DATABASE': DATABASE
        }, offset=offset, limit=limit)
        task.run()
    # Import of Domains
    task = ImportCsvTask(config={
        'QGIS_PATH': r'/home/biegan/Apps/qgis',
        'GPKG_PATH': r'/home/biegan/PycharmProjects/C179-DBIAIT/tasks/tests/data/PBAP_20200203_test.gpkg',
        'DATABASE': DATABASE
    })
    task.run()


if __name__ == "__main__":
    import_gpkg_2()
