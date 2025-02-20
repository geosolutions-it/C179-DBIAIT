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


import logging

logger = logging.getLogger(__name__)

class ShpAcqCheckTask(ShapeChecksBaseTask):
    """
    Dramatiq SHP_ACQ check task definition class.
    """

    @trace_it
    def execute(self, 
                task_id: int,
                *args, 
                **kwargs
                ) -> None:
        
        """
        Method for executing the SHAPE checks
        """
        
        file_year_required = True
        result = False

        try:
            orm_task = Task_CheckShape.objects.get(pk=task_id)
        except ObjectDoesNotExist:
            print(
                f"Task with ID {task_id} was not found! Manual removal had to appear "
                f"between task scheduling and execution."
            )
            raise
        try:         
            # Unpack context_dict into individual variables
            (xlsx_file_uploaded_path,
             dbf_file_uploaded_path,
            ) = args

            SHP_seed = kwargs.get("seed_file", {})
            shp_config = kwargs.get("sheet_mapping_obj", {})
            shp_formulas = kwargs.get("shape_formulas_obj", {})

            with tempfile.TemporaryDirectory() as tmp_dir:
                logger.info(f"Task started with file: {xlsx_file_uploaded_path}")
                tmp_checks_export_dir = pathlib.Path(tmp_dir)
                
                result = ShapeCalc(orm_task, 
                                  xlsx_file_uploaded_path,
                                  dbf_file_uploaded_path,
                                  SHP_seed, 
                                  shp_config, 
                                  shp_formulas,
                                  tmp_checks_export_dir,
                                  file_year_required,
                                  task_progress = 25,
                                  ).run()
            
                # zip the final file
                if result:
                    logger.info(f"Task started with DBF file: {dbf_file_uploaded_path}")
                    file_year_required = False
                    # zip final output in export directory
                    export_file = os.path.join(settings.CHECKS_EXPORT_FOLDER, f"checks_task_{orm_task.id}")
                    shutil.make_archive(export_file, "zip", tmp_checks_export_dir)
                    logger.info(f"Zip created")

            return result
        
        except Exception as e:
            print(f"Error processing files in the background: {e}")