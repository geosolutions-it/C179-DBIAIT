from sympy import Symbol
from sympy.parsing.sympy_parser import parse_expr


class Domain:
    """
    Define a domain table to decode value from GIS to NETSIC environment
    """
    def __init__(self, name):
        """
        name: name of the domain
        """
        self.name = name
        self.items = {}

    def addItem(self, value_gis, value_netsic):
        """
        Add un item to the domain
        values_gis: value in the gis environment
        values_netsci: value in the netsic environment
        """
        self.items[value_gis] = value_netsic

    def decode(self, value_gis):
        """
        Decode the domain from the value in GIS to the value in NETSIC
        value_gis: value to decode in the NETSIC environment
        """
        if value_gis in self.items:
            return self.items[value_gis]
        return None
# ------------------------------------------------


class DomainList:
    """
    List of domains
    """
    def __init__(self):
        """
        Constructor
        """
        self.domains = {}

    def add(self, domain):
        """
        Add un item to the domain list
        domain: domain to add in the list
        """
        self.domains[domain.name] = domain

    def get(self, name):
        """
        Return a domain by its name
        """
        if name in self.domains:
            return self.domains[name]
        return None
# ------------------------------------------------


class BaseTransformation:

    def __init__(self, args=None):
        self.args = args
    
    def apply(self):
        """
        Apply the transformation        
        """
        return None
# ------------------------------------------------


class ConstTransformation:

    def __init__(self, value):
        BaseTransformation.__init__(self, value)
        
    def apply(self):
        """
        Apply the transformation        
        """
        if isinstance(self.args, dict) and "value" in self.args:
            return self.args["value"]
        return None
# ------------------------------------------------


class EmptyTransformation(ConstTransformation):

    def __init__(self):
        value = {"value": ""}
        ConstTransformation.__init__(self, value)
# ------------------------------------------------


class DirectTransformation(BaseTransformation):

    def apply(self):
        """
        Apply the transformation        
        """
        result = None
        if isinstance(self.args, dict):
            if "row" in self.args and "field_name" in self.args:
                row = self.args["row"]
                if isinstance(row, dict) and self.args["field_name"] in row:
                    result = row[self.args["field_name"]]
        return result
# ------------------------------------------------


class DomainTransformation(DirectTransformation):

    def apply(self):
        """
        Apply the transformation        
        """
        result = None
        value = DirectTransformation.apply(self)
        if "domains" in self.args and "domain_name" in self.args:
            domain = self.args["domains"].get(self.args["domain_name"])
            if domain is not None:
                result = domain.decode(value)
        return result
# ------------------------------------------------


class LstripTransformation(DirectTransformation):

    def __init__(self, value):
        DirectTransformation.__init__(self, value)

    def apply(self):
        """
        Apply the transformation
        """
        result = DirectTransformation.apply(self)
        if result is not None and "char" in self.args:
            result = str(result).lstrip(self.args["char"])
        return result
# ------------------------------------------------


class ExpressionTransformation(DirectTransformation):

    def __init__(self, value):
        DirectTransformation.__init__(self, value)

    def apply(self):
        """
        Apply the transformation
        """
        result = DirectTransformation.apply(self)
        if result is not None and "expr" in self.args:
            f = Symbol("_value_")
            sympy_exp = parse_expr(self.args["expr"])
            result = sympy_exp.evalf(subs={f: result})
        return result
# ------------------------------------------------


class CaseTransformation(DirectTransformation):

    def __init__(self, value):
        DirectTransformation.__init__(self, value)

    def sort_by_case(self, condition):
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

    @staticmethod
    def evaluate(value1, op, value2, res):
        if op == "=":
            if value1 == value2:
                return res
        elif op == ">":
            if value1 > value2:
                return res
        elif op == "<":
            if value1 < value2:
                return res
        elif op == ">=":
            if value1 >= value2:
                return res
        elif op == "<=":
            if value1 <= value2:
                return res
        return None

    def _transform(self, cond, result):
        conditions = cond
        conditions.sort(key=self.sort_by_case, reverse=False)
        for cond in conditions:
            l_cond = cond["case"].lower()
            if l_cond == "when":
                value = CaseTransformation.evaluate(result, cond["operator"], cond["value"], cond["result"])
                if value is not None:
                    result = value
                    break
            elif l_cond == "else":
                result = cond["result"]
                break
        return result

    def apply(self):
        """
        Apply the transformation
        """
        result = DirectTransformation.apply(self)
        if result is not None and "cond" in self.args:
            result = self._transform(self.args["cond"], result)
        return result
# ------------------------------------------------


class IfTransformation(CaseTransformation):

    def __init__(self, value):
        self._value = value
        CaseTransformation.__init__(self, value)

    def apply(self):
        """
        Apply the transformation
        """
        result = DirectTransformation.apply(self)
        if result is not None and "cond" in self.args:
            cond = self.args["cond"]
            conditions = [
                {"case": "when", "operator": cond["operator"], "value": cond["value"], "result": cond["result"]},
                {"case": "else", "result": cond["else"]}
            ]
            result = self._transform(conditions, result)
        return result
# ------------------------------------------------


class TransformationFactory:
    @staticmethod
    def from_name(name, args):
        transformation = None
        u_name = name.upper()
        if u_name == "EMPTY":
            transformation = EmptyTransformation()
        elif u_name == "CONST":
            transformation = ConstTransformation(args)
        elif u_name == "DIRECT":
            transformation = DirectTransformation(args)
        elif u_name == "DOMAIN":
            transformation = DomainTransformation(args)
        elif u_name == "LSTRIP":
            transformation = LstripTransformation(args)
        elif u_name == "EXPR":
            transformation = ExpressionTransformation(args)
        elif u_name == "CASE":
            transformation = CaseTransformation(args)
        elif u_name == "IF":
            transformation = IfTransformation(args)
        return transformation
# ------------------------------------------------


if __name__ == "__main__":

    d_list = DomainList()
    domain = Domain("D_AFFIDABILITA")
    domain.addItem("chp", "CHRISTIAN")
    d_list.add(domain)

    tr = TransformationFactory.from_name("EMPTY", None)
    print("empty is " + str(tr.apply()))

    tr = TransformationFactory.from_name("CONST", {"value": 3003})
    print("const is " + str(tr.apply()) )

    row = {"name": "chp"}
    tr = TransformationFactory.from_name("DIRECT", {"row": row, "field_name": "name"})
    print("direct is " + str(tr.apply()))

    tr = TransformationFactory.from_name("DOMAIN", {"row": row, "field_name": "name", "domains": d_list, "domain_name": "D_AFFIDABILITA"})
    print("domain is " + str(tr.apply()))

    row = {"code": "000123"}
    tr = TransformationFactory.from_name("LSTRIP", {"row": row, "field_name": "code", "char": "0"})
    print("lstrip is " + str(tr.apply()))

    row = {"total": 1234.5}
    tr = TransformationFactory.from_name("EXPR", {"row": row, "field_name": "total", "expr": "_value_*1000/365/3600/24"})
    print("expression is " + str(tr.apply()))

    row = {"id_materiale": "MUR"}
    tr = TransformationFactory.from_name("CASE", {"row": row, "field_name": "id_materiale", "cond": [
        {"case": "WHEN", "operator": "=", "value": "MUR", "result": 1},
        {"case": "else", "result": 4},
        {"case": "when", "operator": "=", "value": "CA",  "result": 2},
        {"case": "when", "operator": "=", "value": "PIE", "result": 3}
    ]})
    print("Case is " + str(tr.apply()))

    row = {"id_materiale": "MURO"}
    tr = TransformationFactory.from_name("IF", {"row": row, "field_name": "id_materiale", "cond": {"operator": "=", "value": "MUR", "result": 1, "else": 2}})
    print("If is " + str(tr.apply()))

    row = {"diametro": 123.5}
    tr = TransformationFactory.from_name("IF", {"row": row, "field_name": "diametro",
                                                "cond": {"operator": ">", "value": 100, "result": 1, "else": 2}})
    print("If (diametro-1) is " + str(tr.apply()))

    row = {"diametro": 83.5}
    tr = TransformationFactory.from_name("IF", {"row": row, "field_name": "diametro",
                                                "cond": {"operator": ">", "value": 100, "result": 1, "else": 2}})
    print("If (diametro-2) is " + str(tr.apply()))