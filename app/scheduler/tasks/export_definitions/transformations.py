import ast
import re
import schema

from typing import Dict
from sympy import Symbol
from sympy.parsing.sympy_parser import parse_expr

from app.scheduler.utils import COMPARISON_OPERATORS_MAPPING

from .domains_parser import Domains
from .municipalities_parser import Municipalities
from .exceptions import ExportConfigError

SUPPORTED_TRANSFORMATIONS = [
    "EMPTY",
    "CONST",
    "DIRECT",
    "DOMAIN",
    "DECODE_COM",
    "LSTRIP",
    "EXPR",
    "IF",
    "CASE",
    "ROUND"
]


class BaseTransformation:

    # configuration parameters schema (used for validation json configuration file)
    schema = schema.Schema(schema.Or(None, {}))

    def __init__(self, args=None):
        self.schema.validate(args)
        self.args = args

    def apply(self, *args, **kwargs):
        """
        Apply the transformation
        """
        raise NotImplementedError


class ConstTransformation(BaseTransformation):

    schema = schema.Schema({"value": object})

    def apply(self, **kwargs):
        return self.args.get("value", None)


class EmptyTransformation(BaseTransformation):

    schema = schema.Schema(None)

    def apply(self, **kwargs):
        return ""


class DirectTransformation(BaseTransformation):

    schema = schema.Schema({"field": str})

    def apply(self, row: Dict, **kwargs):
        return row.get(self.args["field"], None)


class DomainTransformation(BaseTransformation):

    schema = schema.Schema({"field": str, "domain_name": str})

    def apply(self, row: Dict, domains: Domains, municipalities: Municipalities, **kwargs):
        x = 5
        field_value = row.get(self.args["field"], None)
        return domains.translate(self.args["domain_name"], field_value)


class DecodeComTransformation(BaseTransformation):

    schema = schema.Schema({"field": str})

    def apply(self, row: Dict, domains: Domains, municipalities: Municipalities, **kwargs):
        field_value = row.get(self.args["field"], None)
        return municipalities.translate_code(field_value)


class LstripTransformation(BaseTransformation):

    schema = schema.Schema(
        {"field": str, "char": schema.And(str, lambda char: len(char) == 1)}
    )

    def apply(self, row: Dict, **kwargs):
        return str(row.get(self.args["field"], None)).lstrip(self.args["char"])


class RoundTransformation(BaseTransformation):

    schema = schema.Schema(
        {"field": schema.Or(str, float), "ndigits": int}
    )

    def apply(self, row: Dict, **kwargs):
        value = row.get(self.args["field"], None)
        if isinstance(row, str):
            value = ast.literal_eval(value)
        return round(value, self.args["ndigits"])


class ExpressionTransformation(BaseTransformation):

    schema = schema.Schema({"field": str, "expr": str})

    def apply(self, row: Dict, **kwargs):
        result = row.get(self.args["field"], None)
        if result is not None:
            f = Symbol("_value_")
            sympy_exp = parse_expr(self.args["expr"])
            result = sympy_exp.evalf(subs={f: result})

        try:
            return float(result)
        except:
            return result


class CaseTransformation(BaseTransformation):

    schema = schema.Schema(
        {
            "field": str,
            "cond": [
                schema.Or(
                    {"case": str, "operator": str, "value": object, "result": object},
                    {"case": str, "result": object},
                    only_one=False,
                )
            ],
        }
    )

    @staticmethod
    def sort_by_case(condition):
        try:
            cond = condition["case"].lower()
            if cond == "when":
                return 0
            elif cond == "else":
                return 1
            else:
                return 2
        except:
            return 3

    def apply(self, row: Dict, **kwargs):
        field_value = row.get(self.args["field"], None)
        conditions = self.args["cond"]
        conditions.sort(key=self.sort_by_case, reverse=False)

        for cond in conditions:
            l_cond = cond["case"].lower()
            if l_cond == "when":
                operator = COMPARISON_OPERATORS_MAPPING.get(cond["operator"], None)
                if operator is not None:
                    if operator(field_value, cond["value"]):
                        return cond["result"]

            elif l_cond == "else":
                return cond["result"]


class IfTransformation(BaseTransformation):
    """
    If transformation, supporting results and else values of other field of the row.

    If "result" and/or "else" keys are assigned with strings, containing: "{ column_alias }",
    value of the column will be returned instead of the string literal.
    """

    schema = schema.Schema(
        {
            "field": str,
            "cond": {
                "operator": str,
                "value": object,
                "result": object,
                "else": object,
            },
        }
    )

    re_pattern = re.compile('{\W*(\w+)\W*}')

    def apply(self, row: Dict, **kwargs):
        field_value = row.get(self.args["field"], None)
        cond = self.args["cond"]
        operator = COMPARISON_OPERATORS_MAPPING.get(cond["operator"], None)

        result_val = cond["result"]
        if isinstance(result_val, str):
            result_alias = re.match(self.re_pattern, result_val)
            if result_alias is not None:
                if result_alias.group(1).isnumeric():
                    result_val = kwargs['already_transformed_data'].get(result_alias.group(1), None)
                else:
                    result_val = row.get(result_alias.group(1), None)

        else_val = cond["else"]
        if isinstance(else_val, str):
            else_alias = re.match(self.re_pattern, else_val)

            if else_alias is not None:
                if else_alias.group(1).isnumeric():
                    else_val = kwargs['already_transformed_data'].get(else_alias.group(1), None)
                else:
                    else_val = row.get(else_alias.group(1), None)

        return result_val if operator(field_value, cond["value"]) else else_val


class TransformationFactory:
    @staticmethod
    def from_name(name, params):
        u_name = name.upper()

        if u_name == "EMPTY":
            transformation = EmptyTransformation()
        elif u_name == "CONST":
            transformation = ConstTransformation(params)
        elif u_name == "DIRECT":
            transformation = DirectTransformation(params)
        elif u_name == "DOMAIN":
            transformation = DomainTransformation(params)
        elif u_name == "DECODE_COM":
            transformation = DecodeComTransformation(params)
        elif u_name == "LSTRIP":
            transformation = LstripTransformation(params)
        elif u_name == "EXPR":
            transformation = ExpressionTransformation(params)
        elif u_name == "CASE":
            transformation = CaseTransformation(params)
        elif u_name == "IF":
            transformation = IfTransformation(params)
        elif u_name == "ROUND":
            transformation = RoundTransformation(params)
        else:
            raise ExportConfigError(f"Unknown transformation method '{name.upper()}'")

        return transformation
