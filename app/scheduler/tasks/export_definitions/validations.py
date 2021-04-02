import ast
import re
from datetime import datetime

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

    def validate(self, value, ref_year):
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

    def validate(self, row: Dict, ref_year: int = None):
        conditions = self.args["cond"]
        result = []
        for cond in conditions:
            and_conditions = cond.get("and", [])
            or_conditions = cond.get("or", [])
            result.append(self._validate_condition(and_conditions, or_conditions, row, ref_year))

        return any(result)

    def _validate_condition(self, and_conditions, or_conditions, row, ref_year):
        and_result = list(self._validate_list(and_conditions, row, ref_year))
        or_result = list(self._validate_list(or_conditions, row, ref_year))

        if len(and_result) > 0 and len(or_result) > 0:
            return all([and_result, any(or_result)])
        elif len(or_result) > 0:
            return all([any(or_result), all(and_result)])
        elif len(and_result) > 0 and len(or_result) == 0:
            return all(and_result)

    def _validate_list(self, conditions, row, ref_year):
        for cond in conditions:
            field_value = row.get(self.args["field"], None)
            if "lookup" in cond:
                lookup_field = re.match(self.re_pattern, cond['lookup'])

                if lookup_field is not None:
                    field_value = row.get(lookup_field.group(1), None)

            if cond['value'] == "{REF_YEAR}":
                cond['value'] = ref_year or datetime.utcnow().year

            if isinstance(cond["value"], str) and "{" in cond['value']:
                lookup_value = re.match(self.re_pattern, cond['value'])

                if lookup_value is not None:
                    value = row.get(lookup_value.group(1), None)

                cond["value"] = self.cast_field(value)

            field_value = self.cast_field(field_value)

            if field_value is None:
                return False

            operator = COMPARISON_OPERATORS_MAPPING.get(cond["operator"], None)
            member_1 = field_value
            member_2 = cond["value"]
            if isinstance(field_value, str) and isinstance(field_value, str):
                member_1 = field_value.upper().strip()
                member_2 = cond["value"].upper().strip() if cond["value"] else cond["value"]

            yield operator(member_1, member_2)

    def cast_field(self, field_value):
        if not isinstance(field_value, int) and field_value is not None and not isinstance(field_value, float):
            try:
                if field_value == '':
                    return field_value
                field_value = ast.literal_eval(field_value)
            except Exception as e:
                pass
        if isinstance(field_value, datetime):
            field_value = field_value.year
        return field_value


class ValidationFactory:
    @staticmethod
    def from_name(name, params):
        u_name = name.upper()

        if u_name == "IF":
            validation = IfValidation(params)
        else:
            raise ExportConfigError(f"Unknown validation method '{name.upper()}'")

        return validation
