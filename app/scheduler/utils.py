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
