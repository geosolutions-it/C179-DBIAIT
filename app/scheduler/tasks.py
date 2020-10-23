from typing import Optional, Dict
from dramatiq import GenericActor
from django.contrib.auth import get_user_model
from django.db.models import Q, ObjectDoesNotExist
import pathlib
from scheduler import exceptions
from .models import GeoPackage, Task, TaskStatus, Schema
import datetime


class TaskType:
    IMPORT = 'IMPORT'
    PROCESS = 'PROCESS'
    EXPORT = 'EXPORT'
    FREEZE = 'FREEZE'


class BaseTask(GenericActor):
    """
    Abstract Dramatiq Task definition class, extended with a pre_send() check, before queuing the Task.

    Inheriting class should ALWAYS define the following methods and class attributes:
    - pre_send()
    - execute()
    - task_type: str
    - name: str

    Usage is similar to the classic dramatiq.GenericActor, with the only difference that Task.send() arguments pattern
    should match the pattern defined by Task.pre_send() method of inheriting class, instead of Task.perform() method.

    For more information please check: https://dramatiq.io/reference.html#class-based-actors

    try:
        Task.send(*args, **kwargs)
    except scheduler.exceptions.QueuingCriteriaViolated as e:
        pass

    """

    # --- Abstract methods and parameters ---

    task_type = None
    name = None

    def pre_send(self, *args, **kwargs) -> int:
        """
        Method executing logic before the Task is queued for execution.

        This function should implement the following:
            1. check whether the conditions are met for the task to be queued. To prevent queuing a task,
               scheduler.exceptions.QueuingCriteriaViolated exception should be raised
            2. Task (ORM model's) instance creation for this task

        :param args: arguments should be explicitly defined by inheriting class's method
        :param kwargs: arguments should be explicitly defined by inheriting class's method
        :raises: QueuingConditionError in case Task queuing criteria is not met
        :returns: Task ORM model ID
        """
        raise NotImplementedError

    def execute(self, task: Task, *args, **kwargs) -> None:
        """
        Method containing logic to be executed in the queued operation.

        :param task: Task ORM model instance
        :param args: execution arguments passed to the Task instance as params['args']
        :param kwargs: execution keyword arguments passed to the Task instance as params['kwargs']
        :return:
        """
        raise NotImplementedError

    # --- Dramatiq Actor functionality ---

    class Meta:
        max_retries = 1
        abstract = True

    def send(self, *args, **kwargs):
        """
        Method used to queue task execution.

        :param args: Arguments passed to this function should match arguments pattern required by self.pre_send() method
        :param kwargs: Keyword arguments passed to this function should match arguments pattern required by self.pre_send() method
        """
        task_id = self.pre_send(*args, **kwargs)
        return self.__actor__.send(task_id)

    def perform(self, task_id: int) -> None:
        """
        Function passed to the background executor.

        :param task_id: Task ORM model ID for this task execution
        """
        try:
            task = Task.objects.get(pk=task_id)
        except ObjectDoesNotExist:
            print(f'Task with provided ID does not exist: {task_id}')
            raise

        task.start_date = datetime.datetime.now()
        task.status = TaskStatus.RUNNING
        task.save()

        try:
            self.execute(task, *task.params.get('args', []), **task.params.get('kwargs', {}))
        except Exception as e:
            print(f'Task failed with an exception: {e}')
            task.status = TaskStatus.FAILED
            task.save()
        else:
            task.status = TaskStatus.SUCCESS
            task.save()
        finally:
            task.end_date = datetime.datetime.now()


class ImportTask(BaseTask):
    """
    Dramatiq Import task definition class.

    Example usage:
        user = User.objects.get(...)
        gpkg_path = pathlib.Path(...)
        additional_execution_params = {'args':[...], 'kwargs': {...}}

        try:
            ImportTask.send(requesting_user=user, gpkg_path=gpkg_path, params=additional_execution_params)
        except scheduler.exceptions.QueuingCriteriaViolated as e:
            logger.error('Scheduling criteria violated for Import task')
    """

    task_type = TaskType.IMPORT
    name = 'import'
    schema = Schema.ANALYSIS

    def pre_send(
            self,
            requesting_user: get_user_model(),
            gpkg_path: pathlib.Path,
            params: Optional[Dict] = None,
         ):

        # 1. check if the Task may be queued
        if not gpkg_path.exists():
            raise exceptions.SchedulingParametersError(f"Provided *.gpkg file does not exist: {gpkg_path.absolute()}")

        colliding_tasks = Task.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
        ).exclude(
            Q(schema=Schema.ANALYSIS) & Q(type=TaskType.EXPORT)
        )

        if len(colliding_tasks) > 0:
            raise exceptions.QueuingCriteriaViolated(f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}")

        # 2. create Task ORM model instance for this task execution
        geopackage = GeoPackage(name=gpkg_path.name)
        geopackage.save()

        current_task = Task(
            requesting_user=requesting_user,
            schema=self.schema,
            geopackage=geopackage,
            type=self.task_type,
            name=self.name,
            params=params['kwargs'].update({'gpkg': gpkg_path.absolute()}),
        )
        current_task.save()

        return current_task.id

    def execute(self, task: Task, gpkg: str = None) -> None:
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


class ExportTask(BaseTask):
    """
    Dramatiq Export task definition class.

    Example usage:
        user = User.objects.get(...)
        additional_execution_params = {'args':[...], 'kwargs': {...}}

        try:
            ExportTask.send(requesting_user=user, schema=Schema.ANALYSIS, params=additional_execution_params)
        except scheduler.exceptions.QueuingCriteriaViolated as e:
            logger.error('Scheduling criteria violated for Import task')
    """

    task_type = TaskType.EXPORT
    name = 'export'

    def pre_send(
            self,
            requesting_user: get_user_model(),
            schema: str = Schema.ANALYSIS,
            params: Optional[Dict] = None,
         ):

        # 1. check if the Task may be queued
        if schema == Schema.ANALYSIS:
            colliding_tasks = Task.objects.filter(
                Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
            ).filter(
                Q(type=TaskType.IMPORT) | Q(type=TaskType.PROCESS)
            )
        elif schema == Schema.FREEZE:
            colliding_tasks = Task.objects.filter(
                Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
            ).filter(
                type=TaskType.FREEZE
            )
        else:
            raise exceptions.SchedulingParametersError(f'Unknown schema: "{schema}"')

        if len(colliding_tasks) > 0:
            raise exceptions.QueuingCriteriaViolated(f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}")

        # 2. create Task ORM model instance for this task execution
        try:
            # get the latest imported package
            latest_import_task = Task.objects.filter(type=TaskType.IMPORT).filter(status=TaskStatus.SUCCESS).latest('end_date')
            geopackage = latest_import_task.geopackage
        except ObjectDoesNotExist:
            raise exceptions.SchedulingParametersError('No imported packages found.')

        current_task = Task(
            requesting_user=requesting_user,
            schema=self.schema,
            geopackage=geopackage,
            type=self.task_type,
            name=self.name,
            params=params if params else {},
        )
        current_task.save()

        return current_task.id

    def execute(self, task: Task, *args, **kwargs) -> None:
        """
        This function should contain the actual code exporting data
        """
        pass


