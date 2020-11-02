import os
import sys
from django_dramatiq.management.commands.rundramatiq import Command as OrigianlRundramatiqCmd


class Command(OrigianlRundramatiqCmd):
    """
    Fix for rundramatiq command, enabling it to work with global interpreter.
    """

    def _resolve_executable(self, exec_name):
        return exec_name
