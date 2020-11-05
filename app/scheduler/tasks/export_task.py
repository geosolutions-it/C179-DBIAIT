import json

from django.contrib.auth import get_user_model
from django.db.models import Q, ObjectDoesNotExist

from app.scheduler import exceptions
from app.scheduler.models import Task
from app.scheduler.tasks.export_definitions.export_shapefile import ShapeExporter
from app.scheduler.utils import Schema, TaskType, TaskStatus

from .base_task import BaseTask


class ExportTask(BaseTask):
    """
    Dramatiq Export task definition class.

    Example usage:
        user = User.objects.get(...)

        try:
            ExportTask.send(ExportTask.pre_send(requesting_user=user, schema=Schema.ANALYSIS))
        except scheduler.exceptions.QueuingCriteriaViolated as e:
            logger.error('Scheduling criteria violated for Import task')
    """

    task_type = TaskType.EXPORT
    name = "export"

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        schema: str = Schema.ANALYSIS,
    ):

        # 1. check if the Task may be queued
        if schema == Schema.ANALYSIS:
            colliding_tasks = Task.objects.filter(
                Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
            ).filter(Q(type=TaskType.IMPORT) | Q(type=TaskType.PROCESS))
        elif schema == Schema.FREEZE:
            colliding_tasks = Task.objects.filter(
                Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
            ).filter(type=TaskType.FREEZE)
        else:
            raise exceptions.SchedulingParametersError(f'Unknown schema: "{schema}"')

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
            schema=schema,
            geopackage=geopackage,
            type=cls.task_type,
            name=cls.name,
        )
        current_task.save()

        return current_task.id

    def execute(self, task_id: int, *args, **kwargs) -> None:
        """
        This function should contain the actual code exporting data
        """
        with open("/Applications/QGIS3.12.app/Contents/Resources/python/C179-DBIAIT/shapefile.json", "r") as f:
            exports = json.load(f)
        for export in exports:
            exporter = ShapeExporter(task_id, export["source"]["table"], export["name"], export["folder"], export["source"]["fields"])
            exporter.execute()
