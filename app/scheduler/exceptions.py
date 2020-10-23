
class SchedulerException(Exception):
    pass


class QueuingCriteriaViolated(SchedulerException):
    pass


class SchedulingParametersError(SchedulerException):
    pass
