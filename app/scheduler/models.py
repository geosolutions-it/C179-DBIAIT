import os
import uuid
from pathlib import Path
from postgres_copy import CopyManager

from app.scheduler.utils import (
    TaskStatus,
    default_storage,
    status_icon_mapper,
    style_class_mapper,
)
from django.db import models
from django.conf import settings
from django.contrib.auth import get_user_model


class GeoPackage(models.Model):
    name = models.CharField(max_length=50, blank=False)

    def __str__(self):
        return self.name


class Task(models.Model):
    uuid = models.UUIDField(default=uuid.uuid4, unique=True)
    requesting_user = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)
    schema = models.CharField(max_length=250)
    geopackage = models.ForeignKey(
        GeoPackage, on_delete=models.CASCADE, blank=True, null=True
    )
    type = models.CharField(max_length=50)
    name = models.CharField(max_length=300)
    start_date = models.DateTimeField(blank=True, null=True)
    end_date = models.DateTimeField(blank=True, null=True)
    status = models.CharField(max_length=20, null=False, default=TaskStatus.QUEUED)
    logfile = models.CharField(max_length=300, blank=True, default=None)
    params = models.JSONField(
        help_text="Task arguments.", blank=True, default=default_storage
    )
    progress = models.IntegerField(default=0)

    def save(self, *args, **kwargs):
        if not self.logfile:
            self.logfile = os.path.join(
                settings.BASE_DIR, "task_logs", f"{self.type}_{self.uuid}.log"
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

    @property
    def task_log(self):
        if os.path.exists(self.logfile):
            task_log = Path(self.logfile).read_text()
            if task_log == "(True,)\n":
                return "Task completed successfully"
            return task_log


class AllDomains(models.Model):
    """
    "all_domains" table model used for a quick *.csv data loading

    The table will be searched for in schemas defined by 'default' database's OPTION "-c search_path".
    It should always point at ANALYSIS schema (directly after 'PUBLIC' schema), for proper data loading.
    """

    dominio_gis = models.CharField(max_length=50, null=False)
    valore_gis = models.CharField(max_length=100, null=False)
    descrizione_gis = models.CharField(max_length=255, null=True)
    dominio_netsic = models.CharField(max_length=50, null=True)
    valore_netsic = models.CharField(max_length=100, null=True)
    objects = CopyManager()

    class Meta:
        managed = False
        db_table = "all_domains"
