import math
import logging
import pathlib

from datetime import datetime
from django.db import connections

from app.scheduler.models import Task
from app.scheduler.utils import dictfetchall, translate_schema_to_db_alias


class ExportBase:

    def __init__(
        self, export_dir: pathlib.Path, orm_task: Task, max_progress: int = 100
    ):
        """
        Initialization function of data export

        Parameters:
            export_dir: directory where export and log files should be stored
            orm_task: instance of the database Task reporting execution of this export task (default 100)
            max_progress: max value of Task's progress, which should be set after a successful export
        """
        self.export_dir = export_dir
        self.orm_task = orm_task
        self.max_progress = max_progress
        self.starting_progress = orm_task.progress
        self.logger = None

        # make sure target location exists
        self.export_dir.parent.mkdir(parents=True, exist_ok=True)

    def configure_file_logger(self):
        """
        Method configuring logger for logging user dedicated errors of the export into a specified location.
        """
        today = datetime.today()

        logfile_path = pathlib.Path(
            self.export_dir.absolute(), f"logfile_{today.strftime('%Y%m%d')}.log"
        )
        logger = logging.getLogger(__name__)
        hdlr = logging.FileHandler(logfile_path.absolute())
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.INFO)

        self.logger = logger

    def update_progress(self, step, total_steps):
        self.orm_task.progress = math.floor(
            step * (self.max_progress - self.starting_progress) / total_steps
        )
        self.orm_task.save()

    def set_max_progress(self):
        self.orm_task.progress = self.max_progress
        self.orm_task.save()

    def execute_pre_process(self, pre_process: str):
        """
        Method executing SQL procedure, before exporting datas
        """
        if pre_process is not None:
            with connections[
                translate_schema_to_db_alias(self.orm_task.schema)
            ].cursor() as cursor:
                cursor.callproc(f"{self.orm_task.schema}.{pre_process}")

            return cursor.fetchone()
