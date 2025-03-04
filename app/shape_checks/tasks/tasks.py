import os
import tempfile
import pathlib
import shutil

from django.db.models import ObjectDoesNotExist
from django.conf import settings

from app.shape_checks.tasks.shape_checks_base_task import ShapeChecksBaseTask
from app.shape_checks.tasks.checks_definitions.shape_calc import ShapeCalc
from app.shape_checks.models import Task_CheckShape


from app.scheduler.tasks.base_task import trace_it



class ShpAcqCheckTask(ShapeChecksBaseTask):
    """
    Dramatiq SHP_ACQ check task class.
    """
    sheet_for_dbf = "SHP_Acquedotto"

class ShpFgnCheckTask(ShapeChecksBaseTask):
    """
    Dramatiq SHP_FFN check task definition class.
    """
    sheet_for_dbf = "SHP_Fognatura"