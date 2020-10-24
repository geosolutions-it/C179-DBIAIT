import os
import uuid

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import models


class TaskStatus:
    QUEUED = 'QUEUED'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    SUCCESS = 'SUCCESS'


style_class_mapper = {
    TaskStatus.QUEUED: u"table-primary",
    TaskStatus.FAILED: u"table-danger",
    TaskStatus.RUNNING: u"table-info",
    TaskStatus.SUCCESS: u"table-success"
}

status_icon_mapper = {
    TaskStatus.QUEUED: u"fas fa-circle text-warning",
    TaskStatus.FAILED: u"fas fa-times-circle text-danger",
    TaskStatus.RUNNING: u"fas fa-sync fa-spin text-primary",
    TaskStatus.SUCCESS: u"fas fa-check-circle text-success"
}


def default_storage():
    return {}


class GeoPackage(models.Model):
    name = models.CharField(max_length=50, blank=False)

    def __str__(self):
        return self.name


class Task(models.Model):
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    requesting_user = models.ForeignKey(
        get_user_model(), on_delete=models.CASCADE)
    schema = models.CharField(max_length=250)
    geopackage = models.ForeignKey(GeoPackage, on_delete=models.CASCADE)
    type = models.CharField(max_length=50)
    name = models.CharField(max_length=50)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    status = models.CharField(
        max_length=20, null=False, default=TaskStatus.QUEUED)
    logfile = models.CharField(max_length=300, blank=True, default=None)
    params = models.JSONField(
        help_text='Task arguments.', blank=True, default=default_storage)
    progress = models.IntegerField(default=0)

    def save(self, *args, **kwargs):
        if not self.logfile:
            self.logfile = os.path.join(
                settings.BASE_DIR, 'task_logs', f"{self.type}_{self.uuid}.log"
            )

        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.type}:{self.name}"

    @property
    def style_class(self):
        return style_class_mapper.get(self.status)

    @property
    def status_icon(self):
        return status_icon_mapper.get(self.status)
