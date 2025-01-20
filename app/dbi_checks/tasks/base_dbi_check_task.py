import datetime
from dramatiq import GenericActor

def time_it(message):
    print("-" * 80)
    now = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    print(f"[{now}] - {message}")
    print("-" * 80)


def trace_it(func):
    def inner(*args, **kwargs):
        time_it("Task STARTED")
        result = func(*args, **kwargs)
        time_it("Task COMPLETED")
        return result
    return inner


class BaseTask(GenericActor):
    """
    Abstract Dramatiq Task definition class.

    Inheriting class should ALWAYS define the following methods and class attributes:
    - name: str
    - task_type: str
    - pre_send(): class method
    - execute(): method

    Usage is similar to the classic dramatiq.GenericActor, with the only difference that Task.send() takes a single
    argument - ORM Task ID, which is created by pre_send() class method.

    For more information please check: https://dramatiq.io/reference.html#class-based-actors

    try:
        Task.send(Task.pre_send(*args, **kwargs))
    except scheduler.exceptions.QueuingCriteriaViolated as e:
        pass

    """

    # --- Abstract methods and parameters ---

    task_type = None
    name = None

    @classmethod
    def pre_send(cls, *args, **kwargs) -> int:
        """
        Method executing logic before the Task is queued for execution.

        This function should implement the following:
            1. check whether the conditions are met for the task to be queued. To prevent the task from queuing
               scheduler.exceptions.QueuingCriteriaViolated exception should be raised
            2. create Task (ORM model's) instance for this task and return it's ID

        :param args: arguments should be explicitly defined by inheriting class's method
        :param kwargs: arguments should be explicitly defined by inheriting class's method
        :raises: QueuingConditionError in case Task queuing criteria is not met
        :returns: Task ORM model ID
        """
        raise NotImplementedError

    def execute(self, task_id: int, *args, **kwargs) -> None:
        """
        Method containing logic to be executed in the queued operation.

        :param task_id: ORM Task instance ID
        :param args: execution arguments passed to the Task instance as params['args'], declared in self.pre_send() on Task instance creation
        :param kwargs: execution keyword arguments passed to the Task instance as params['kwargs'], declared in self.pre_send() on Task instance creation
        """
        raise NotImplementedError
    
    def perform(self, task_id: int, *args, **kwargs) -> None:
        
        raise NotImplementedError

    # --- Dramatiq Actor functionality ---

    class Meta:
        max_retries = 0
        abstract = True