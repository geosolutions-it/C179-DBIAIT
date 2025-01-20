import os
import uuid
import datetime

from django.db import models
from django.conf import settings
from django.contrib.auth import get_user_model

from app.scheduler.utils import TaskStatus, status_icon_mapper, style_class_mapper

class Xlsx(models.Model):
    name = models.CharField(max_length=50, blank=False, unique=True)

    def __str__(self):
        return self.name

class Task_CheckDbi(models.Model):
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    requesting_user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    schema = models.CharField(max_length=250)
    xlsx = models.ForeignKey(
        Xlsx, on_delete=models.CASCADE, blank=True, null=True
    )
    type = models.CharField(max_length=50)
    name = models.CharField(max_length=300)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    status = models.CharField(max_length=20, null=False, default=TaskStatus.QUEUED)
    logfile = models.CharField(max_length=300, blank=True, default=None)

    def save(self, *args, **kwargs):
        if not self.logfile:
            self.logfile = os.path.join(
                settings.BASE_DIR, "task_check_dbi_logs", f"{self.type}_{self.uuid}.log"
            )

        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.type}:{self.name}"

    @property
    def user(self):
        return self.requesting_user.username
    
    @property
    def style_class(self):
        return style_class_mapper.get(self.status, "")

    @property
    def status_icon(self):
        return status_icon_mapper.get(self.status, "")

class ImportedSheet(models.Model):
    task = models.ForeignKey(Task_CheckDbi, on_delete=models.CASCADE)
    import_start_timestamp = models.DateTimeField(default=datetime.datetime.now)
    import_end_timestamp = models.DateTimeField(null=True)
    sheet_name = models.CharField(max_length=250, null=False)
    status = models.CharField(max_length=20, null=False, default=TaskStatus.QUEUED)

    def to_dict(self):
        return {
            "task": str(self.task.uuid),
            "import_start_timestamp": str(self.import_start_timestamp),
            "import_end_timestamp": str(self.import_end_timestamp),
            "sheet_name": self.sheet_name,
            "status": self.status
        }