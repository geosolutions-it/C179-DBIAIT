import os
import uuid
import datetime
from pathlib import Path
from enum import IntEnum

from django.db import models
from django.conf import settings
from django.contrib.auth import get_user_model

from app.dbi_checks.utils import CheckType
from app.scheduler.utils import TaskStatus, status_icon_mapper, style_class_mapper, default_storage
    
class Xlsx(models.Model):
    name = models.CharField(max_length=300, blank=False, unique=True)
    file_path = models.CharField(max_length=300, blank=False, null=True)
    second_file_path = models.CharField(max_length=300, blank=False, null=True)
    analysis_year = models.CharField(max_length=10, blank=False, default=f"Current")


    def __str__(self):
        return f"{self.name}: {self.file_path}, {self.second_file_path}"

class Task_CheckDbi(models.Model):
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    requesting_user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    schema = models.CharField(max_length=250)
    xlsx = models.ForeignKey(
        Xlsx, on_delete=models.CASCADE, blank=True, null=True
    )
    imported = models.BooleanField(blank=True, null=False, default=False)
    exported = models.BooleanField(blank=True, null=False, default=False)
    check_type = models.CharField(
        max_length=5,
        default=CheckType.CDO,
    )
    name = models.CharField(max_length=100)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    status = models.CharField(max_length=20, null=False, default=TaskStatus.QUEUED)
    logfile = models.CharField(max_length=300, blank=True, default=None)
    progress = models.IntegerField(default=0)

    def save(self, *args, **kwargs):
        if not self.logfile:
            self.logfile = os.path.join(
                settings.BASE_DIR, "task_check_dbi_logs", f"{self.name}_{self.uuid}.log"
            )

        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.name}"

    @property
    def user(self):
        return self.requesting_user.username
    
    @property
    def style_class(self):
        return style_class_mapper.get(self.status, "")

    @property
    def status_icon(self):
        return status_icon_mapper.get(self.status, "")
    
    @property
    def task_log(self):
        if os.path.exists(self.logfile):
            task_log = Path(self.logfile).read_text()
            if task_log == "(True,)\n":
                return "Task completed successfully"
            return task_log.replace("\n", "<br/>").replace("(True,)", "SUCCESS").replace("(False,)", "FAILED")
        

class ProcessType(IntEnum):
    COPY = 1
    CALCULATION = 2
    SAVE = 3
    LOG = 4

    @classmethod
    def get_label(cls, value):
        labels = {
            cls.COPY: "copia",
            cls.CALCULATION: "calcolo",
            cls.SAVE: "salvataggio",
            cls.LOG: "log"
        }
        return labels.get(value, "Unknown Process")

    @classmethod
    def choices(cls):
        return [(member.value, cls.get_label(member.value)) for member in cls]

class ProcessState(models.Model):
    task = models.ForeignKey(Task_CheckDbi, on_delete=models.CASCADE)
    process_type = models.PositiveSmallIntegerField(choices=ProcessType.choices())
    sheet_name = models.CharField(max_length=100, null=False)
    file_name = models.CharField(max_length=100, null=False, default="")
    import_start_timestamp = models.DateTimeField(default=datetime.datetime.now)
    import_end_timestamp = models.DateTimeField(null=True)
    status = models.CharField(max_length=20, null=False, default=TaskStatus.QUEUED)

    def to_dict(self):
        return {
            "task": str(self.task.uuid),
            "process_type": ProcessType.get_label(self.process_type),
            "sheet_name": self.sheet_name,
            "file_name": self.file_name,
            "import_start_timestamp": str(self.import_start_timestamp),
            "import_end_timestamp": str(self.import_end_timestamp),
            "status": self.status
        }