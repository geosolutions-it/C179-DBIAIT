def default_storage():
    return {"args": [], "kwargs": {}}


class Schema:
    ANALYSIS = "analysis"
    FREEZE = "freeze"


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
