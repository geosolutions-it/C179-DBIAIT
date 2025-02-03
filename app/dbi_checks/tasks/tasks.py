import os
import tempfile
import pathlib
import shutil
import dramatiq

from django.db.models import Q, ObjectDoesNotExist
from django.contrib.auth import get_user_model
from django.conf import settings

from app.dbi_checks.utils import YearHandler
from app.dbi_checks.models import Task_CheckDbi, Xlsx
from app.dbi_checks.tasks.checks_definitions.base_calc import BaseCalc
from app.dbi_checks.tasks.checks_base_task import ChecksBaseTask

from app.scheduler.tasks.base_task import trace_it
from app.scheduler.utils import TaskStatus, Schema
from app.scheduler import exceptions

import logging

logger = logging.getLogger(__name__)

class ConsistencyCheckTask(ChecksBaseTask):
    """
    Dramatiq Consistency check task definition class.
    """

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        file_path1: str,
        file_path2: str,
        name: str
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
                                                   file_path=file_path1,
                                                   second_file_path=file_path2,
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
            name=name,
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
            # Unpack context_dict into individual variables
            (xlsx_file1_uploaded_path,
             xlsx_file2_uploaded_path,
             DBI_A,
             DBI_A_1,
             dbi_a_config,
             dbi_a_1_config,
             dbi_a_formulas,
             dbi_a_1_formulas
            ) = args

            year_required = kwargs.get("year_required", False)

            with tempfile.TemporaryDirectory() as tmp_dir:
                logger.info(f"Task started with file: {xlsx_file1_uploaded_path}")
                tmp_checks_export_dir = pathlib.Path(tmp_dir)
                
                result = BaseCalc(orm_task, 
                                  xlsx_file1_uploaded_path, 
                                  DBI_A, 
                                  dbi_a_config, 
                                  dbi_a_formulas,
                                  tmp_checks_export_dir,
                                  year_required,
                                  ).run()
            
                # Copy the second file using the DBI_A_1 seed only if the first copy is completed
                if result:
                    logger.info(f"Task started with file: {xlsx_file2_uploaded_path}")
                    year_required = False
                    BaseCalc(orm_task, 
                             xlsx_file2_uploaded_path, 
                             DBI_A_1, 
                             dbi_a_1_config, 
                             dbi_a_1_formulas,
                             tmp_checks_export_dir,
                             year_required,
                             ).run()
                    # zip final output in export directory
                    export_file = os.path.join(settings.CHECKS_EXPORT_FOLDER, f"checks_task_{orm_task.id}")
                    shutil.make_archive(export_file, "zip", tmp_checks_export_dir)
                    logger.info(f"Zip created")
                    result = True

            return result
        
        except Exception as e:
            print(f"Error processing files in the background: {e}")

class PrioritizedDataCheckTask(ChecksBaseTask):
    """
    Dramatiq PrioritizedData check task definition class.

    """
    
    @trace_it
    @dramatiq.actor(time_limit=3600000)  # Set max_age for 1 hour in milliseconds
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
            # Unpack context_dict into individual variables
            (xlsx_file_uploaded_path,
             DATI_PRIORITATI,
             dbi_prior_config,
             dbi_prior_formulas
            ) = args

            with tempfile.TemporaryDirectory() as tmp_dir:
                logger.info(f"Task started with file: {xlsx_file_uploaded_path}")
                tmp_checks_export_dir = pathlib.Path(tmp_dir)
                
                result = BaseCalc(orm_task, 
                                  xlsx_file_uploaded_path, 
                                  DATI_PRIORITATI, 
                                  dbi_prior_config, 
                                  dbi_prior_formulas,
                                  tmp_checks_export_dir
                                  ).run()
            
                # Copy the second file using the DBI_A_1 seed only if the first copy is completed
                if result:
                    # zip final output in export directory
                    export_file = os.path.join(settings.CHECKS_EXPORT_FOLDER, f"checks_task_{orm_task.id}")
                    shutil.make_archive(export_file, "zip", tmp_checks_export_dir)
                    logger.info(f"Zip created")
                    result = True

            return result
        
        except Exception as e:
            print(f"Error processing files in the background: {e}")

            

    