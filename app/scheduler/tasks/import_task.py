import math
import pathlib

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Q, ObjectDoesNotExist

from app.scheduler import exceptions
from app.scheduler.models import GeoPackage, Task
from app.scheduler.utils import Schema, TaskType, TaskStatus

from .base_task import BaseTask
from .import_definitions.base_import import initQgis
from .import_definitions.import_gpkg import GpkgImportDefinition
from .import_definitions.import_csv import CsvImportDefinition


class ImportTask(BaseTask):
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

    task_type = TaskType.IMPORT
    name = "import"
    schema = Schema.ANALYSIS

    max_tables_per_import_run = 50

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        gpkg_name: str,
    ):

        # 1. check if the Task may be queued
        gpkg_path = pathlib.Path(settings.IMPORT_FOLDER, gpkg_name)
        if not gpkg_path.exists():
            raise exceptions.SchedulingParametersError(
                f"Provided *.gpkg file does not exist: {gpkg_path.absolute()}"
            )

        colliding_tasks = Task.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
        ).exclude(Q(schema=Schema.ANALYSIS) & Q(type=TaskType.EXPORT))

        if len(colliding_tasks) > 0:
            raise exceptions.QueuingCriteriaViolated(
                f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}"
            )

        # 2. create Task ORM model instance for this task execution
        geopackage = GeoPackage(name=gpkg_path.name)
        geopackage.save()

        current_task = Task(
            requesting_user=requesting_user,
            schema=cls.schema,
            geopackage=geopackage,
            type=cls.task_type,
            name=cls.name,
            params={"kwargs": {"gpkg_path": str(gpkg_path.absolute())}},
        )
        current_task.save()

        return current_task.id

    def execute(self, task_id: int, *args, gpkg_path: str = None, **kwargs) -> None:
        """
        This function executes the logic of the Import Task.

        Note: import must be divided in steps, since maximum number of layers imported by QGis library in one run
        is limited. By default the limit is 50 tables.
        """
        print(f"Starting IMPORT execution of package from: {gpkg_path}")

        try:
            orm_task = Task.objects.get(pk=task_id)
        except ObjectDoesNotExist:
            print(
                f"Task with ID {task_id} was not found! Manual removal had to appear "
                f"between task scheduling and execution."
            )
            raise

        # get *.gpkg file's feature classes based on the configuration file
        feature_classes = GpkgImportDefinition.get_feature_classes()
        total_feature_classes_number = len(feature_classes)
        import_steps_number = math.ceil(
            total_feature_classes_number / self.max_tables_per_import_run
        )

        qgs, processing, postgis = initQgis()

        for step in range(import_steps_number):

            offset = step * self.max_tables_per_import_run
            limit = self.max_tables_per_import_run
            # Import of Feature Classes
            gpkg_import = GpkgImportDefinition(
                gpkg_path=gpkg_path,
                offset=offset,
                limit=limit,
                qgs=qgs,
                processing=processing,
                postgis=postgis,
            )
            gpkg_import.run()

            orm_task.progress = math.floor(step * 100 / (import_steps_number + 1))
            orm_task.save()

        CsvImportDefinition.run()

        orm_task.progress = 100
        orm_task.save()

        print(f"Finished IMPORT execution of package from: {gpkg_path}")
