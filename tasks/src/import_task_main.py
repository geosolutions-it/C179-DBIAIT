import os
import math
import subprocess
from import_task import ImportTask

MAX_TABLES_PER_RUN = 50
DIR_NAME = os.path.dirname(__file__)


def import_gpkg():
    fc = ImportTask.get_feature_classes()
    tot_fc = len(fc)
    steps = math.ceil(tot_fc/MAX_TABLES_PER_RUN)
    for i in range(steps):
        offset = i*MAX_TABLES_PER_RUN
        limit = MAX_TABLES_PER_RUN
        subprocess.call("python " + DIR_NAME + "/import_task.py " + str(offset) + " " + str(limit), shell=False)


if __name__ == "__main__":
    import_gpkg()
