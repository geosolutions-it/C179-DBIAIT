import datetime

from app.scheduler import exceptions
from app.scheduler.models import Process, ProcessHistory, Task
from app.scheduler.tasks.base_task import BaseTask
from app.scheduler.utils import Schema, TaskStatus, TaskType
from django.contrib.auth import get_user_model
from django.db.models import Q


class ProcessTask(BaseTask):
    """
    Dramatiq Import task definition class.

    Example usage:
        user = User.objects.get(...)
        process = Process

        try:
            ProcessTask.send(ProcessTask.pre_send(requesting_user=user, process=process_object))
        except scheduler.exceptions.QueuingCriteriaViolated as e:
            logger.error('Scheduling criteria violated for Import task')
    """

    task_type = TaskType.PROCESS
    name = "process"
    schema = Schema.ANALYSIS

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        process: Process,
    ):

        # 1. check if the Task may be queued
        if not Process:
            raise exceptions.SchedulingParametersError(
                f"Process with [ID={Process.pk}] does not exist."
            )

        colliding_tasks = Task.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
        ).exclude((Q(schema=Schema.ANALYSIS) & Q(type=TaskType.PROCESS)) | (Q(schema=Schema.FREEZE) & Q(type=TaskType.EXPORT)))

        if len(colliding_tasks) > 0:
            raise exceptions.QueuingCriteriaViolated(
                f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}"
            )

        current_task = Task.objects.create(
            requesting_user=requesting_user,
            schema=cls.schema,
            type=cls.task_type,
            name=cls.name,
            params={'kwargs': {}},
            start_date=datetime.datetime.now()
        )
        process = ProcessHistory.objects.create(
            process=process, task=current_task)

        return current_task.pk

    def execute(self, task_id: int, *args, gpkg_path: str = None, **kwargs) -> None:
        """
        This function calls the process in a stored procedure
        """
        process_history = ProcessHistory.objects.get(task_id=task_id)
        print(process_history.run_process_algorith())
