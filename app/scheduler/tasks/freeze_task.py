from django.contrib.auth import get_user_model
from django.db.models import Q, ObjectDoesNotExist

from app.scheduler import exceptions
from app.scheduler.models import Task
from app.scheduler.utils import Schema, TaskType, TaskStatus

from .base_task import BaseTask


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

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
    ):

        # 1. check if the Task may be queued
        colliding_tasks = Task.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
        ).exclude(Q(schema=Schema.ANALYSIS) & Q(type=TaskType.EXPORT))

        if len(colliding_tasks) > 0:
            raise exceptions.QueuingCriteriaViolated(
                f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}"
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
        )
        current_task.save()

        return current_task.id

    def execute(self, task_id: int, *args, **kwargs) -> None:
        """
        This function should contain the actual code freezing data
        """
        pass
