import schema
from .exceptions import ExportConfigError
from app.scheduler.utils import COMPARISON_OPERATORS_MAPPING

SUPPORTED_VALIDATIONS = ["IF"]


class BaseValidation:

    # configuration parameters schema (used for validation json configuration file)
    schema = schema.Schema(schema.Or(None, {}))

    def __init__(self, args=None):
        self.schema.validate(args)
        self.args = args

    def validate(self, value):
        """
        Apply the transformation
        """
        raise NotImplementedError


class IfValidation(BaseValidation):

    schema = schema.Schema(
        {
            "cond": {
                "operator": str,
                "value": object,
            },
        }
    )

    def validate(self, value):
        cond = self.args["cond"]
        operator = COMPARISON_OPERATORS_MAPPING.get(cond["operator"], None)

        return True if operator(value, cond["value"]) else False


class ValidationFactory:
    @staticmethod
    def from_name(name, params):
        u_name = name.upper()

        if u_name == "IF":
            validation = IfValidation(params)
        else:
            raise ExportConfigError(f"Unknown validation method '{name.upper()}'")

        return validation
