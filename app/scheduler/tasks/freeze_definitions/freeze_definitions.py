from django.utils import timezone

from app import settings
from app.scheduler.models import Task, FreezeLayer
from app.scheduler.tasks.base_task import trace_it
from app.scheduler.tasks.freeze_definitions.base_freeze import BaseFreezeDefinition
from app.scheduler.utils import Schema, TaskStatus
from django.db import connections, connection


class FreezeDefinition(BaseFreezeDefinition):
    def __init__(
            self,
            orm_task: Task,
            offset=0,
            limit=50,
            schema=Schema.FREEZE,
            current_year=None,
            notes=None
    ):
        super().__init__(schema=schema)

        self.orm_task = orm_task
        self.offset = offset
        self.schema = schema
        self.limit = limit
        self.current_year = current_year
        self.notes = notes

    @staticmethod
    def get_freeze_layers():
        layers = connections['analysis'].introspection.table_names()  # TODO check su come rimuovere public dalla ricerca delle tabelle
        return layers

    def run(self):
        layer_to_freeze = self.get_freeze_layers()
        cont = self.offset
        n_step = len(layer_to_freeze)
        if n_step > 0:
            for layername in layer_to_freeze[self.offset: self.offset + self.limit]:
                cont += 1
                print(layername + ": " + str(cont))
                start_date = timezone.now()
                end_date = None
                task_status = TaskStatus.RUNNING
                try:
                    self._run_freeze_procedure(layername.lower())
                    task_status = TaskStatus.SUCCESS
                    end_date = timezone.now()
                except Exception as e:
                    print(layername + ": " + str(e))
                    task_status = TaskStatus.FAILED
                finally:
                    FreezeLayer.objects.create(
                        task=self.orm_task,
                        layer_name=layername.lower(),
                        freeze_start_timestamp=start_date,
                        freeze_end_timestamp=end_date,
                        status=task_status
                    )
            return (cont / n_step) * 100

    def _run_freeze_procedure(self, layer):
        print(f"PROCESSING {layer}...")
        freeze_cursor = connection.cursor()
        with freeze_cursor as cursor:
            kparam = {"v_table_name": layer, "v_year": self.current_year, "v_note": self.notes}
            cursor.callproc(
                f"{settings.DATABASE_SCHEMAS['freeze']}.initialize_freeze_table", kparam)
            result = cursor.fetchone()
        print(f"procedure {settings.DATABASE_SCHEMAS['freeze']}.initialize_freeze_table => {result}")
        return result
