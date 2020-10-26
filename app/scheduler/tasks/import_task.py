import pathlib

from django.contrib.auth import get_user_model
from django.db.models import Q

from app.scheduler import exceptions
from app.scheduler.models import GeoPackage, Task
from app.scheduler.utils import Schema, TaskType, TaskStatus

from .base_task import BaseTask


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

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        gpkg_path: pathlib.Path,
    ):

        # 1. check if the Task may be queued
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
            params={'kwargs': {'gpkg_path': str(gpkg_path.absolute())}}
        )
        current_task.save()

        return current_task.id

    def execute(self, task_id: int, *args, gpkg_path: str = None, **kwargs) -> None:
        """
        This function should contain the actual code for gpkg import

        general example:
            geopackage = pathlib.Path(gpkg)

            if not geopackage.exists():
                raise Exception('Geopackage not found')

            layers = get_feature_classes(geopackage)
            for layer in layers:
                import_layer(layer)
                task.progress = int(layer/layers*100)
                task.save()
        """
        pass
