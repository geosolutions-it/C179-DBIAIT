import os
import math
import subprocess
from import_task import ImportTask
import _thread

MAX_TABLES_PER_RUN = 50
DIR_NAME = os.path.dirname(__file__)


def import_gpkg(testo):
    print("testo: " + testo)
    fc = ImportTask.get_feature_classes()
    tot_fc = len(fc)
    steps = math.ceil(tot_fc/MAX_TABLES_PER_RUN)
    for i in range(steps):
        offset = i*MAX_TABLES_PER_RUN
        limit = MAX_TABLES_PER_RUN
        #Import of Feature Classes
        subprocess.call("python " + DIR_NAME + "/import_task.py " + str(offset) + " " + str(limit), shell=False)
    #Import of Domains
    subprocess.call("python " + DIR_NAME + "/import_csv.py", shell=False)

def import_domain(testo):
    print("testo: " + testo)
    subprocess.call("python " + DIR_NAME + "/import_csv.py", shell=False)

if __name__ == "__main__":
    import_gpkg()
    #_thread.start_new_thread(import_gpkg, ("GPKG",))
    #_thread.start_new_thread(import_domain, ("DOMAIN",))
    #while 1:
    #   pass