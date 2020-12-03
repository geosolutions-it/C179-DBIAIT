import re

import schema
from .exceptions import ExportConfigError
from app.scheduler.utils import COMPARISON_OPERATORS_MAPPING
from typing import Dict

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
    """
        If validation, will check if a specific value match the condition.
        The conditions can be multiple in AND or OR (or both means that both conditions must be True)
        The validation also accept templated field that must be added into a specific key called "lookup"
        with "{ column_alias }" as alias.
        Simple example with AND:
        {
            "field": "foo_field",
            "cond": [{
                "and": [
                    {"operator": ">", "value": 2},
                    {"operator": "<", "value": 10},
                ]
            }],
        }
        return True if the value of "foo_field" is greater than 2 and lower than 10

        Simple example with OR:
        {
            "field": "foo_field",
            "cond": [{
                "or": [
                    {"operator": ">", "value": 2},
                    {"operator": "<", "value": 10},
                ]
            }],
        }
        return True if the value of "foo_field" is greater than 2 OR lower than 10

        Simple example with AND, OR and templated field:
        {
            "field": "foo_field",
            "cond": [{
                "and": [
                    {"lookup": "{ bar_field }", "operator": ">", "value": 2},
                    {"operator": "<", "value": 10},
                ],
                "or": [
                    {"lookup": ">", "value": 3},
                    {"operator": "<", "value": 25},
                ]
            }],
        }
        return True if
        - the value of "bar_field" is greater than 2
        - if the value of bar_field is lower than 10
        OR if the value of "foo_field" is greater than 3 or lower than 25"
    """

    schema = schema.Schema(
        {
            "field": str,
            "cond":[
                schema.And(
                    {str: [{schema.Optional('lookup'): str, "operator": str, "value": object}]}
                )]
        }
    )
    re_pattern = re.compile('{\W*(\w+)\W*}')

    def validate(self, row: Dict):
        conditions = self.args["cond"]
        result = []
        for cond in conditions:
            and_conditions = cond.get("and", [])
            or_conditions = cond.get("or", [])
            result.append(self._validate_condition(and_conditions, or_conditions, row))

        return all(result)

    def _validate_condition(self, and_conditions, or_conditions, row):
        and_result = list(self._validate_list(and_conditions, row))
        or_result = list(self._validate_list(or_conditions, row))

        if len(and_result) > 0 and len(or_result) > 0:
            return all([and_result, any(or_result)])
        elif len(or_result) > 0:
            return all([any(or_result), all(and_result)])
        elif len(and_result) > 0 and len(or_result) == 0:
            return all(and_result)

    def _validate_list(self, conditions, row):
        field_value = row.get(self.args["field"], None)
        for cond in conditions:
            if "lookup" in cond:
                lookup_field = re.match(self.re_pattern, cond['lookup'])

                if lookup_field is not None:
                    field_value = row.get(lookup_field.group(1), None)

            operator = COMPARISON_OPERATORS_MAPPING.get(cond["operator"], None)
            yield operator(field_value, cond["value"])


class ValidationFactory:
    @staticmethod
    def from_name(name, params):
        u_name = name.upper()

        if u_name == "IF":
            validation = IfValidation(params)
        else:
            raise ExportConfigError(f"Unknown validation method '{name.upper()}'")

        return validation
