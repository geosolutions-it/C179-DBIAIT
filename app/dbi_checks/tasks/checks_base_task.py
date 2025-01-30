import pathlib
import traceback

from django.db.models import ObjectDoesNotExist
from django.utils import timezone

from app.dbi_checks.models import Task_CheckDbi, ImportedSheet 

from app.scheduler.tasks.base_task import BaseTask
from app.scheduler.utils import TaskStatus
from app.scheduler.logging import Tee

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
                    *context_data["args"],
                    **context_data["kwargs"]
                    )

        except Exception as exception:
            task.status = TaskStatus.FAILED
            task.save()

            traceback_info = "".join(
                traceback.TracebackException.from_exception(exception).format()
            )
            print(traceback_info)

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
            Final check of the ImportedSheet.
            If at least 1 sheet process is failed, the whole task is considered unsuccessful
            '''
            import_sheet_exists = ImportedSheet.objects.filter(task_id__id=task.id).exists()

            if import_sheet_exists:
                # If at least one instance exists, check if all have status 'SUCCESS'
                import_sheet = ImportedSheet.objects.filter(task_id__id=task.id)
                imported_results = all(sheet.status == 'SUCCESS' for sheet in import_sheet)
    
                # Set task status based on the results
                task.status = TaskStatus.SUCCESS if imported_results else TaskStatus.FAILED

            # After the complete of the process, change the Task_CheckDbi exported field to True
            task.exported = True

            task.progress = 100
            task.end_date = timezone.now()
            task.save()