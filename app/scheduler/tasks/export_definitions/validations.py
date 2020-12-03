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
            "cond":
                schema.Or(
                    {str: [{"operator": str, "value": object}]}
                )
        }
    )

    def validate(self, value):
        conditions = self.args["cond"]
        and_conditions = conditions.get("and", [])
        or_conditions = conditions.get("or", [])
        and_result = list(self._validate_list(and_conditions, value))
        or_result = list(self._validate_list(or_conditions, value))
        if len(or_result) > 0:
            return any(or_result + and_result)
        elif len(and_result) > 0 and len(or_result) == 0:
            return all(and_result + or_result)

    @staticmethod
    def _validate_list(conditions, value):
        for cond in conditions:
            operator = COMPARISON_OPERATORS_MAPPING.get(cond["operator"], None)
            yield operator(value, cond["value"])


class ValidationFactory:
    @staticmethod
    def from_name(name, params):
        u_name = name.upper()

        if u_name == "IF":
            validation = IfValidation(params)
        else:
            raise ExportConfigError(f"Unknown validation method '{name.upper()}'")

        return validation
