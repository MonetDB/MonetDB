
class var:
	def __init__(self,name):
		self._name = name
		self._values = []

	def add(self,key,value):
		self._values[key] = value

	def append( self, value ):
		self._values.append(value)

	def keys(self):
		return self._values.keys()

	def value(self,key):
		return self._values[key]

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

