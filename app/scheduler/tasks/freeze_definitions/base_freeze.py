from django.conf import settings
from app.scheduler.utils import Schema


class BaseFreezeDefinition:
    def __init__(self, schema=Schema.FREEZE):
        database = settings.DATABASES[settings.TASKS_DATABASE]
        self.database_config = {
            "HOST": database["HOST"],
            "PORT": database["PORT"],
            "DATABASE": database["NAME"],
            "SCHEMA": schema,
            "USERNAME": database["USER"],
            "PASSWORD": database["PASSWORD"],
        }
