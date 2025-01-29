import os
import tempfile
import pathlib
import shutil
import traceback

from django.db.models import Q, ObjectDoesNotExist
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.conf import settings

from app.dbi_checks.utils import YearHandler
from app.dbi_checks.models import Task_CheckDbi, Xlsx, ImportedSheet 
from app.dbi_checks.tasks.checks_definitions.consistency_check import ConsistencyCheck

from app.scheduler.tasks.base_task import BaseTask, trace_it
from app.scheduler.utils import TaskStatus, Schema
from app.scheduler import exceptions
from app.scheduler.logging import Tee

import logging

logger = logging.getLogger(__name__)

class Import_DbiCheckTask(BaseTask):
    """
    Dramatiq Import task definition class.

    Example usage:
        user = User.objects.get(...)
        gpkg_path = pathlib.Path(...)

        try:
            ImportTask.send(ImportTask.pre_send(requesting_user=user, gpkg_path=gpkg_path))
        except scheduler.exceptions.QueuingCriteriaViolated as e:
            logger.error('Scheduling criteria violated for Import task')
    """

    #TODO change the name in order to refer the check name
    name = "import_check_dbi"
    schema = Schema.ANALYSIS

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        file_path1: str,
        file_path2: str,
        seed_a: str,
        seed_a_1: str, 
        dbi_a_config: str,
        dbi_a_1_config: str,
        dbi_a_formulas: str,
        dbi_a_1_formulas: str,
        year_required=False,
    ):

        # Check if the xlsx files exists
        file1 = pathlib.Path(file_path1)
        file2 = pathlib.Path(file_path2)
        if not file1.exists() or not file2.exists():
            raise exceptions.SchedulingParametersError(
                f"Provided *.xlsx files do not exist: {file1.name}, {file2.name}"
            )

        colliding_tasks = Task_CheckDbi.objects.filter(
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

        # Get or create Xlsx ORM model instance for this task execution
        xlsx, created = Xlsx.objects.get_or_create(name=f"{file1.name.split('.')[0]}_{file2.name.split('.')[0]}",
                                                   file_path1=file_path1,
                                                   file_path2=file_path2,
                                                   analysis_year = analysis_year,
                                                   )
        if created:
            xlsx.save()

        # Create Task ORM model instance for this task execution
        current_task = Task_CheckDbi(
            requesting_user=requesting_user,
            schema=cls.schema,
            xlsx=xlsx,
            imported=True,
            name=cls.name,
            params={
                "args": [
                  str(file_path1),
                  str(file_path2),
                  seed_a,
                  seed_a_1,
                  dbi_a_config,
                  dbi_a_1_config,
                  dbi_a_formulas,
                  dbi_a_1_formulas
                ],
                "kwargs": {
                    "year_required": year_required,
                }
            }
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
        Method for executing the DBI checks
        """
        
        result = False

        try:
            orm_task = Task_CheckDbi.objects.get(pk=task_id)
        except ObjectDoesNotExist:
            print(
                f"Task with ID {task_id} was not found! Manual removal had to appear "
                f"between task scheduling and execution."
            )
            raise
        try:
            (
            file_path1,
            file_path2,
            seed_a,
            seed_a_1,
            dbi_a_config,
            dbi_a_1_config,
            dbi_a_formulas,
            dbi_a_1_formulas,
            ) = args
            
            year_required = kwargs.get("year_required")

            with tempfile.TemporaryDirectory() as tmp_dir:
                logger.info(f"Task started with file: {file_path1}")
                tmp_checks_export_dir = pathlib.Path(tmp_dir)
                
                result = ConsistencyCheck(orm_task, 
                                          file_path1, 
                                          seed_a, 
                                          dbi_a_config, 
                                          dbi_a_formulas,
                                          year_required,
                                          tmp_checks_export_dir).run()
            
                # Copy the second file using the DBI_A_1 seed only if the first copy is completed
                if result is True:
                    logger.info(f"Task started with file: {file_path2}")
                    year_required = False
                    ConsistencyCheck(orm_task, 
                                     file_path2, 
                                     seed_a_1, 
                                     dbi_a_1_config, 
                                     dbi_a_1_formulas,
                                     year_required,
                                     tmp_checks_export_dir,
                                     ).run()
                    # zip final output in export directory
                    export_file = os.path.join(settings.CHECKS_EXPORT_FOLDER, f"checks_task_{orm_task.id}")
                    shutil.make_archive(export_file, "zip", tmp_checks_export_dir)
                    logger.info(f"Zip created")
                    result = True

            return result
        
        except Exception as e:
            print(f"Error processing files in the background: {e}")
        
    def perform(self, 
                task_id: int,
                *args,
                **kwargs
                ) -> None:
        """
        This function executes the logic of the Import Task.
        """

        try:
            task = Task_CheckDbi.objects.get(pk=task_id)
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
                    task.id, # we send the task instance
                    *task.params.get("args", []),
                    **task.params.get("kwargs", {}),
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
            Final check of the ImportedSheet.
            If at least 1 sheet process is failed, the whole task is considered unsuccessful
            '''
            import_sheet_exists = ImportedSheet.objects.filter(task_id__id=task.id).exists()

            if import_sheet_exists:
                # If at least one instance exists, check if all have status 'SUCCESS'
                import_sheet = ImportedSheet.objects.filter(task_id__id=task.id)
                imported_results = all(sheet.status == 'SUCCESS' for sheet in import_sheet)
    
                # Set task status based on the results
                task.status = TaskStatus.SUCCESS if imported_results else TaskStatus.FAILED

            # After the complete of the process, change the Task_CheckDbi exported field to True
            task.exported = True

            task.progress = 100
            task.end_date = timezone.now()
            task.save()

    