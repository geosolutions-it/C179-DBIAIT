import math
import pathlib

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models import Q, ObjectDoesNotExist

from app.scheduler import exceptions
from app.scheduler.models import GeoPackage, Task
from app.scheduler.utils import Schema, TaskType, TaskStatus
from django.db import connection

from .base_task import BaseTask, trace_it
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

    max_tables_per_import_run = 25

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
                "Ci sono dei task al momento in secuzione, che impediscono l'avvio del processo di freeze. "
                "Si prega di riprovare più tardi"
                # f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}"
            )

        # 2. get or create GeoPackage ORM model instance for this task execution
        geopackage, created = GeoPackage.objects.get_or_create(name=gpkg_path.name)
        if created:
            geopackage.save()

        # 3. create Task ORM model instance for this task execution
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

    @trace_it
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

        qgs, processing, GdalUtils, isWindows = initQgis()

        for step in range(import_steps_number):

            offset = step * self.max_tables_per_import_run
            limit = self.max_tables_per_import_run
            # Import of Feature Classes
            gpkg_import = GpkgImportDefinition(
                gpkg_path=gpkg_path,
                orm_task=orm_task,
                offset=offset,
                limit=limit,
                qgs=qgs,
                processing=processing,
                GdalUtils=GdalUtils,
                isWindows=isWindows
            )
            progress = gpkg_import.run()

            orm_task.progress = progress
            orm_task.save()

        CsvImportDefinition.run()

        setattr(orm_task, 'progress', 100)
        orm_task.save()

        # grant role to specific users after import status
        print(f"Granting permissions to: {', '.join(settings.DBIAIT_ANL_SELECT_ROLES)}")
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT DBIAIT_ANALYSIS.reset_proc_stda_tables();")
            cursor.execute(
                f"GRANT SELECT ON ALL TABLES IN SCHEMA "
                f"dbiait_analysis TO {' '.join(settings.DBIAIT_ANL_SELECT_ROLES)};")
            cursor.execute("VACUUM ANALYZE VERBOSE;")

        print(f"Finished IMPORT execution of package from: {gpkg_path}")
