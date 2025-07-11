import os
import pathlib
import traceback
import shutil
import tempfile
from openpyxl.workbook import Workbook

from django.db.models import Q, ObjectDoesNotExist
from django.conf import settings
from django.contrib.auth import get_user_model
from django.utils import timezone

from app.shape_checks.models import Task_CheckShape, ShapeCheckProcessState, XlsxDbf
from app.shape_checks.tasks.checks_definitions.shape_calc import ShapeCalc

from app.dbi_checks.utils import YearHandler

from app.scheduler.tasks.base_task import BaseTask
from app.scheduler.utils import TaskStatus, Schema
from app.scheduler.logging import Tee
from app.scheduler import exceptions
from app.scheduler.tasks.base_task import trace_it

import logging

logger = logging.getLogger(__name__)

class ShapeChecksBaseTask(BaseTask):
    """
    This class inherits from the scheduler.base_task.BaseTask
    which is inherits from GenericActor
    """

    class Meta:
        max_retries = 0
        abstract = True

    schema = Schema.ANALYSIS
    sheet_for_dbf = None

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        file_path1: str,
        file_path2: str,
        name: str,
        check_type: str,
        group: str,
    ):

        # Check if the xlsx files exists
        file1 = pathlib.Path(file_path1)
        file2 = pathlib.Path(file_path2)
        if not file1.exists() or not file2.exists():
            raise exceptions.SchedulingParametersError(
                f"Provided *.xlsx or *dbf files do not exist: {file1.name}, {file2.name}"
            )
        colliding_tasks = Task_CheckShape.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
        ).exclude(Q(schema=Schema.ANALYSIS))

        if len(colliding_tasks) > 0:
            raise exceptions.QueuingCriteriaViolated(
                "Qualcosa è andato storto durante il caricamento o l'elaborazione"
                "Si prega di riprovare più tardi"
                # f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}"
            )
        
        # Get analysis year
        analysis_year = YearHandler(file1).get_year()

        if not analysis_year:
            raise exceptions.SchedulingParametersError(
                f"L'anno di analisi non è presente nel file caricato. L'anno deve essere presente nella cella B8 del foglio DATI per il file DBI_A"
            )

        # Get or create Xlsx ORM model instance for this task execution
        xlsx_dbf, created = XlsxDbf.objects.get_or_create(name=f"{file1.name.split('.')[0]}_{file2.name.split('.')[0]}",
                                                          file_path=file_path1,
                                                          second_file_path=file_path2,
                                                          analysis_year = analysis_year,
                                                          )
        if created:
            xlsx_dbf.save()

        # Create Task ORM model instance for this task execution
        current_task = Task_CheckShape(
            requesting_user=requesting_user,
            schema=cls.schema,
            xlsx_dbf=xlsx_dbf,
            imported=True,
            check_type=check_type,
            name=name,
            group=group,
        )
        current_task.save()

        return current_task.id
    
    @trace_it
    def execute(self, 
                task_id: int,
                *args, 
                **kwargs
                ) -> None:
        
        """
        Method for executing the SHAPE checks
        """
        
        file_year_required = False
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
                
                # Create a single log workbook
                log_workbook = Workbook()
                
                result = ShapeCalc(orm_task, 
                                  xlsx_file_uploaded_path,
                                  dbf_file_uploaded_path,
                                  self.sheet_for_dbf,
                                  SHP_seed, 
                                  shp_config, 
                                  shp_formulas,
                                  tmp_checks_export_dir,
                                  file_year_required,
                                  task_progress = 20,
                                  log_workbook = log_workbook
                                  ).run()
            
                # zip the final file
                if result:

                    # Save logs only once after both runs
                    log_workbook.save(tmp_checks_export_dir / "logs.xlsx")
                    # zip final output in export directory
                    export_file = os.path.join(settings.CHECKS_EXPORT_FOLDER, f"checks_task_{orm_task.id}")
                    shutil.make_archive(export_file, "zip", tmp_checks_export_dir)
                    logger.info(f"Zip created")

            return result
        
        except Exception as e:
            print(f"Error processing files in the background: {e}")
    
    def perform(self, 
                task_id: int,
                context_data: dict
                ) -> None:
        """
        This function executes the logic of the Import Task.
        """

        try:
            task = Task_CheckShape.objects.get(pk=task_id)
        except ObjectDoesNotExist:
            print(
                f"Task with ID {task_id} was not found!"
            )
            raise

        task.start_date = timezone.now()
        task.status = TaskStatus.RUNNING
        task.save()

        logfile = pathlib.Path(task.logfile)
        result = False
        try:
            
            # create task's log directory
            logfile.parent.mkdir(parents=True, exist_ok=True)

            with Tee(logfile, "a"):
                result = self.execute(
                    task.id,
                    *context_data.get("args", []),
                    **context_data.get("kwargs", {})
                    )

        except Exception as exception:
            task.status = TaskStatus.FAILED
            task.save()

            traceback_info = "".join(
                traceback.TracebackException.from_exception(exception).format()
            )
            print(traceback_info)

            # try logging the exception
            try:
                with open(task.logfile, "a") as log:
                    log.write(traceback_info)
            except:
                pass
        else:
            task.status = TaskStatus.SUCCESS if result else TaskStatus.FAILED
            task.progress = 100
            task.save()
        finally:
            '''
            Final check of the ProcessState.
            If at least 1 process type is failed, the whole task is considered unsuccessful
            '''
            
            if task.status != TaskStatus.FAILED:
                process_state_exists = ShapeCheckProcessState.objects.filter(task_id__id=task.id).exists()

                if process_state_exists:
                    # If at least one instance exists, check if all have status 'SUCCESS'
                    process_state = ShapeCheckProcessState.objects.filter(task_id__id=task.id)
                    state_results = all(process.status == 'SUCCESS' for process in process_state)
        
                    # Set task status based on the results
                    task.status = TaskStatus.SUCCESS if state_results else TaskStatus.FAILED

            # After the complete of the process, change the Task_CheckDbi exported field to True
            task.exported = True

            task.progress = 100
            task.end_date = timezone.now()
            task.save()