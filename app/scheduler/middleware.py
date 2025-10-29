import os
from dramatiq.middleware import TimeLimit
from django.conf import settings


class CustomTimeLimit(TimeLimit):
    """
    We had to override the current and default middleware because
    for how the projects use dramatiq there is no easy or clear way
    to increase the timeout limit or the interval time.
    In this way via end we are able to increase the values

    Original descriptions:
        Middleware that cancels actors that run for too long.
        Currently, this is only available on CPython.

        Note:
        This works by setting an async exception in the worker thread
        that runs the actor.  This means that the exception will only get
        called the next time that thread acquires the GIL.  Concretely,
        this means that this middleware can't cancel system calls.

        Parameters:
        time_limit(int): The maximum number of milliseconds actors may
            run for.
        interval(int): The interval (in milliseconds) with which to
            check for actors that have exceeded the limit.
    """

    def __init__(self, *, time_limit=settings.DRAMATIQ_TIMEOUT_LIMIT, interval=settings.DRAMATIQ_TIMEOUT_INTERVAL):
        super().__init__(time_limit=time_limit, interval=interval)
