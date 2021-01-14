import operator
from django.conf import settings

from .exceptions import SchedulingParametersError


def default_storage():
    return {"args": [], "kwargs": {}}


class Schema:
    ANALYSIS = settings.DATABASE_SCHEMAS.get('analysis')
    FREEZE = settings.DATABASE_SCHEMAS.get('freeze')
    SYSTEM = settings.DATABASE_SCHEMAS.get('system')


class TaskType:
    IMPORT = "IMPORT"
    PROCESS = "PROCESS"
    EXPORT = "EXPORT"
    FREEZE = "FREEZE"


class TaskStatus:
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCESS = "SUCCESS"


COMPARISON_OPERATORS_MAPPING = {
    "=": operator.eq,
    ">": operator.gt,
    "<": operator.lt,
    ">=": operator.ge,
    "<=": operator.le,
    "!=": operator.ne
}

style_class_mapper = {
    TaskStatus.QUEUED: u"table-primary",
    TaskStatus.FAILED: u"table-danger",
    TaskStatus.RUNNING: u"table-info",
    TaskStatus.SUCCESS: u"table-success"
}

status_icon_mapper = {
    TaskStatus.QUEUED: u"fas fa-circle text-warning icon-status",
    TaskStatus.FAILED: u"fas fa-times-circle text-danger icon-status",
    TaskStatus.RUNNING: u"fas fa-sync fa-spin text-primary icon-status",
    TaskStatus.SUCCESS: u"fas fa-check-circle text-success icon-status"
}


def dictfetchall(cursor):
    """
    Fetch all result rows of a raw SQL query from Django connection.cursor() as dicts
    """
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def translate_schema_to_db_alias(schema: str):
    """
    Function returning db_alias based on the selected schema. Used for managing database connections
    in multiple schema environment.
    """
    db_aliases = [key for key, value in settings.DATABASE_SCHEMAS.items() if value == schema]

    if len(db_aliases) != 1:
        raise SchedulingParametersError(
            f'Error while translating schema "{schema}" to db_alias from settings.DATABASE_SCHEMAS'
        )

    return db_aliases[0]


def translate_schema_to_enum(schema: str):
    """
    Function returningenum based on the selected schema.
    """
    db_aliases = [key for key, value in settings.DATABASE_SCHEMAS.items() if value == schema]

    if len(db_aliases) != 1:
        raise SchedulingParametersError(
            f'Error while translating schema "{schema}" to enum from settings.DATABASE_SCHEMAS'
        )
    return settings.DATABASE_SCHEMAS.get(db_aliases[0])


