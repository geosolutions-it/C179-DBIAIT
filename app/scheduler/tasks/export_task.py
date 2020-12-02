import os
import json
import shutil
import pathlib
import tempfile

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Q, ObjectDoesNotExist

from app.scheduler import exceptions
from app.scheduler.models import Task
from app.scheduler.tasks.export_definitions.export_shapefile import ShapeExporter
from app.scheduler.utils import Schema, TaskType, TaskStatus
from app.scheduler.tasks.export_definitions.export_xls import ExportXls
from app.scheduler.tasks.export_definitions.config_scraper import ExportConfig

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

        # 1a. validate export configuration
        ExportConfig()

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

    def export_shapefiles(self, export_directory: pathlib.Path, task: Task):
        with open(settings.SHAPEFILE_EXPORT_CONFIG.substitute(), "r") as f:
            exports = json.load(f)

        for export in exports:
            if not export["skip"]:
                kwargs = {
                    "task_id": task.id,
                    "table": export["source"]["table"],
                    "name": export["name"],
                    "shape_file_folder": pathlib.Path(export_directory, export["folder"]),
                    "fields": export["source"]["fields"],
                    "filter_query": export["source"]["filter"],
                    "pre_process": export["pre_process"],
                }
                exporter = ShapeExporter(**kwargs)
                exporter.execute()
            else:
                print(f"Skipped the export of [name={export[u'name']}] shapefile")

    def execute(self, task_id: int, *args, **kwargs) -> None:
        """
        Method executing data export.
        """

        try:
            orm_task = Task.objects.get(pk=task_id)
        except ObjectDoesNotExist:
            print(
                f"Task with ID {task_id} was not found! Manual removal had to appear "
                f"between task scheduling and execution."
            )
            raise

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_export_directory = pathlib.Path(tmp_dir)

            ExportXls(tmp_export_directory, orm_task, max_progress=90).run()
            self.export_shapefiles(tmp_export_directory, orm_task)

            # zip final output in export directory
            export_file = os.path.join(settings.EXPORT_FOLDER, f"task_{orm_task.id}")
            shutil.make_archive(export_file, "zip", tmp_export_directory)

        orm_task.progress = 100
        orm_task.save()
