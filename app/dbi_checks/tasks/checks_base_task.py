import pathlib
import traceback

from django.db.models import Q, ObjectDoesNotExist
from django.contrib.auth import get_user_model
from django.utils import timezone

from app.dbi_checks.models import Task_CheckDbi, ProcessState, Xlsx
from app.dbi_checks.utils import YearHandler

from app.scheduler.tasks.base_task import BaseTask
from app.scheduler.utils import TaskStatus, Schema
from app.scheduler.logging import Tee
from app.scheduler import exceptions

class ChecksContext:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

class ChecksBaseTask(BaseTask):
    """
    This class inherits from the scheduler.base_task.BaseTask
    which is inherits from GenericActor
    """

    class Meta:
        max_retries = 0
        abstract = True

    schema = Schema.ANALYSIS

    @classmethod
    def pre_send(
        cls,
        requesting_user: get_user_model(),
        file_path: str,
        name: str,
        check_type: str,
    ):

        # Check if the xlsx files exists
        file = pathlib.Path(file_path)
        if not file.exists():
            raise exceptions.SchedulingParametersError(
                f"Provided *.xlsx file does not exist: {file.name}"
            )

        colliding_tasks = Task_CheckDbi.objects.filter(
            Q(status=TaskStatus.QUEUED) | Q(status=TaskStatus.RUNNING)
        ).exclude(Q(schema=Schema.ANALYSIS))

        if len(colliding_tasks) > 0:
            raise exceptions.QueuingCriteriaViolated(
                "Qualcosa è andato storto durante il caricamento o l'elaborazione"
                "Si prega di riprovare più tardi"
                # f"Following tasks prevent scheduling this operation: {[task.id for task in colliding_tasks]}"
            )
        
        # Get analysis year
        analysis_year = YearHandler(file).get_year()
        
        if not analysis_year:
            raise exceptions.SchedulingParametersError(
                f"L'anno di analisi non è presente nel file caricato. L'anno deve essere presente nella cella B8 del foglio DATI per il file DBI_A"
            )

        # Get or create Xlsx ORM model instance for this task execution
        xlsx, created = Xlsx.objects.get_or_create(name=f"{file.name.split('.')[0]}",
                                                   file_path=file_path,
                                                   analysis_year = analysis_year,
                                                   )
        if created:
            xlsx.save()

        # Create Task ORM model instance for this task execution
        current_task = Task_CheckDbi(
            requesting_user=requesting_user,
            schema=cls.schema,
            xlsx=xlsx,
            imported=True,
            check_type=check_type,
            name=name,
        )
        current_task.save()

        return current_task.id
    
    def perform(self, 
                task_id: int,
                context_data: dict
                ) -> None:
        """
        This function executes the logic of the Import Task.
        """

        try:
            task = Task_CheckDbi.objects.get(pk=task_id)
        except ObjectDoesNotExist:
            print(
                f"Task with ID {task_id} was not found!"
            )
            raise

        task.start_date = timezone.now()
        task.status = TaskStatus.RUNNING
        task.save()

        logfile = pathlib.Path(task.logfile)
        result = False
        try:
            
            # create task's log directory
            logfile.parent.mkdir(parents=True, exist_ok=True)

            with Tee(logfile, "a"):
                result = self.execute(
                    task.id,
                    *context_data.get("args", []),
                    **context_data.get("kwargs", {})
                    )

        except Exception as exception:
            task.status = TaskStatus.FAILED
            task.save()

            traceback_info = "".join(
                traceback.TracebackException.from_exception(exception).format()
            )
            #print(traceback_info)

            # try logging the exception
            try:
                with open(task.logfile, "a") as log:
                    log.write(traceback_info)
            except:
                pass
        else:
            task.status = TaskStatus.SUCCESS if result else TaskStatus.FAILED
            task.progress = 100
            task.save()
        finally:
            '''
            Final check of the ProcessState.
            If at least 1 process type is failed, the whole task is considered unsuccessful
            '''
            
            if task.status != TaskStatus.FAILED:
                process_state_exists = ProcessState.objects.filter(task_id__id=task.id).exists()

                if process_state_exists:
                    # If at least one instance exists, check if all have status 'SUCCESS'
                    process_state = ProcessState.objects.filter(task_id__id=task.id)
                    state_results = all(process.status == 'SUCCESS' for process in process_state)
        
                    # Set task status based on the results
                    task.status = TaskStatus.SUCCESS if state_results else TaskStatus.FAILED

            # After the complete of the process, change the Task_CheckDbi exported field to True
            task.exported = True

            task.progress = 100
            task.end_date = timezone.now()
            task.save()
