import operator
from django.conf import settings


def default_storage():
    return {"args": [], "kwargs": {}}


class Schema:
    ANALYSIS = settings.DATABASE_SCHEMAS.get('analysis')
    FREEZE = settings.DATABASE_SCHEMAS.get('freeze')


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
}

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


def dictfetchall(cursor):
    """
    Fetch all result rows of a raw SQL query from Django connection.cursor() as dicts
    """
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]
