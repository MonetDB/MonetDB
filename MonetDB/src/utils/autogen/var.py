
class var:
    def __init__(self,name):
        self._name = name
        self._values = []

    def append( self, value ):
        self._values.append(value)

    def has_val(self,val):
        return val in self._values

    def __repr__(self):
        res = self._name + ": " + repr(self._values)
        return res

class groupvar(var):
    def __init__(self,name):
        self._name = name
        self._values = {}

    def add(self,key,value):
        self._values[key] = value

    def keys(self):
        return self._values.keys()

    def items(self):
        return self._values.items()

    def has_key(self,key):
        return self._values.has_key(key)

    def __getitem__(self,key):
        return self._values[key]
