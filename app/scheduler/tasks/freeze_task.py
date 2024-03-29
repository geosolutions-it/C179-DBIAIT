import math

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import connection
from django.db.models import Q, ObjectDoesNotExist

from app.scheduler import exceptions
from app.scheduler.models import Task, Freeze
from app.scheduler.utils import Schema, TaskType, TaskStatus

from .base_task import BaseTask, trace_it
from .freeze_definitions.freeze_definitions import FreezeDefinition


class FreezeTask(BaseTask):
    """
    Dramatiq Freeze task definition class.

    Example usage:
        user = User.objects.get(...)

        try:
            FreezeTask.send(FreezeTask.pre_send(requesting_user=user, schema=Schema.ANALYSIS))
        except scheduler.exceptions.QueuingCriteriaViolated as e:
            logger.error('Scheduling criteria violated for Import task')
    """

    task_type = TaskType.FREEZE
    name = "freeze"
    schema = Schema.FREEZE
    max_tables_per_import_run = 25

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        ref_year: str,
        notes: str,
    ):

        # 1. check if the Task may be queued
        colliding_tasks = Task.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
        ).exclude(Q(schema=Schema.ANALYSIS) & Q(type=TaskType.EXPORT))

        if len(colliding_tasks) > 0:
            raise exceptions.QueuingCriteriaViolated(
                # f"I seguenti task impediscono l'avvio del task di Freeze: {[task.type for task in colliding_tasks]}"
                "Ci sono dei task al momento in secuzione, che impediscono l'avvio del processo di freeze. Si prega di riprovare più tardi"
            )

        # 2. create Task ORM model instance for this task execution
        try:
            # get the latest imported package
            latest_import_task = (
                Task.objects.filter(type=TaskType.IMPORT)
                .filter(status=TaskStatus.SUCCESS)
                .latest("end_date")
            )
            geopackage = latest_import_task.geopackage
        except ObjectDoesNotExist:
            raise exceptions.SchedulingParametersError("No imported packages found.")

        current_task = Task(
            requesting_user=requesting_user,
            schema=cls.schema,
            geopackage=geopackage,
            type=cls.task_type,
            name=cls.name,
            params={"kwargs": {"ref_year": str(ref_year), "notes": notes}}
        )
        '''
        Before save the task in "queued", the system will check if the configuration files 
        are already present in the export folder. This is needed here because otherwise 
        we will not raise the exception to the user
        '''
        FreezeDefinition(current_task).freeze_configuration_files(year=ref_year)
        '''
        If the file are copied, we will save the task in queued mode
        '''
        current_task.save()

        return current_task.id

    def execute(self, task_id: int, *args, ref_year: str = None, notes: str = None, **kwargs) -> None:
        """
        Freezing data.
        By Default ref_year and notes are passed by **kwargs. For local usage (with execute) pass them as param to the execute command
        """
        print(f"Starting FREEZE execution of of year: {ref_year}")

        try:
            orm_task = Task.objects.get(pk=task_id)
        except ObjectDoesNotExist:
            print(
                f"Task with ID {task_id} was not found! Manual removal had to appear "
                f"between task scheduling and execution."
            )
            raise

        FreezeDefinition(orm_task).delete_freeze_year(ref_year)

        freeze_information = Freeze(
            ref_year=ref_year,
            notes=notes,
            task=orm_task
        )
        freeze_information.save()

        feature_classes = FreezeDefinition.get_freeze_layers()
        total_feature_classes_number = len(feature_classes)
        import_steps_number = math.ceil(
            total_feature_classes_number / self.max_tables_per_import_run
        )

        for step in range(import_steps_number):
            offset = step * self.max_tables_per_import_run
            limit = self.max_tables_per_import_run
            # Import of Feature Classes
            freeze_process = FreezeDefinition(
                  orm_task=orm_task,
                  offset=offset,
                  limit=limit,
                  current_year=ref_year,
                  notes=notes
              )
            progress = freeze_process.run()

            orm_task.progress = progress
            orm_task.save()

        setattr(orm_task, 'progress', 100)
        orm_task.save()

        # grand permissions to specific roles
        print(f"Granting permissions to: {', '.join(settings.DBIAIT_FRZ_SELECT_ROLES + settings.DBIAIT_FRZ_UID_ROLES + settings.DBIAIT_FRZ_ADMIN_ROLES)}")
        with connection.cursor() as cursor:
            cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA dbiait_freeze TO {' '.join(settings.DBIAIT_FRZ_SELECT_ROLES)};")
            cursor.execute(f"GRANT SELECT, UPDATE, INSERT, DELETE ON ALL TABLES IN SCHEMA dbiait_freeze TO {' '.join(settings.DBIAIT_FRZ_UID_ROLES)};")
            cursor.execute(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbiait_freeze TO {' '.join(settings.DBIAIT_FRZ_ADMIN_ROLES)};")

        print(f"Finished FREEZE execution of of year: {ref_year}")
        return True

