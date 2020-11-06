import os
import json
import shutil

from django.conf import settings
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

    def export_shapefiles(self, task_id):
        with open(settings.SHAPEFILE_EXPORT_CONFIG, u"r") as f:
            exports = json.load(f)

        # create temporary export directory if it does not exist
        not os.path.exists(settings.TEMP_EXPORT_DIR) and os.makedirs(settings.TEMP_EXPORT_DIR)
        for export in exports:
            if not export[u"skip"]:
                kwargs = {
                    u"task_id": task_id, 
                    u"table": export[u"source"][u"table"],
                    u"name": export[u"name"],
                    u"shape_file_folder": export[u"folder"],
                    u"fields": export[u"source"][u"fields"],
                    u"filter_query": export[u"source"][u"filter"],
                    u"pre_process": export[u"pre_process"],
                }
                exporter = ShapeExporter(**kwargs)
                exporter.execute()
            else:
                print(f"Skipped the export of [name={export[u'name']}] shapefile")

    def execute(self, task_id: int, *args, **kwargs) -> None:
        """
        This function should contain the actual code exporting data
        """
        self.export_shapefiles(task_id)

        # zip final output in export directory
        task_export_folder = os.path.join(settings.TEMP_EXPORT_DIR, str(task_id))
        export_file = os.path.join(settings.EXPORT_FOLDER, str(task_id))
        results = os.path.exists(task_export_folder) and shutil.make_archive(export_file, u"zip", task_export_folder)
        print(f"zip file creation returned {results}")

