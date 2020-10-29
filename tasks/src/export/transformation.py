

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
        return transformation


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