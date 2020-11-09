from app.scheduler.exceptions import SchedulerException


class ExportConfigError(SchedulerException):
    pass


class TransformationArgumentsError(SchedulerException):
    pass
