# To allow for transferring functions from python to monetdb without the source code we analyze the underlying PyCodeObject structure of a python function object
# We can then convert this PyCodeObject to an encoded string using the Marshal module, and decode it to reconstruct the PyCodeObject in MonetDB

# This is necessary because Python throws away the source code of functions created in the interpreter
#   so if we want to pass an arbitrary function to MonetDB we need to transfer it using its underlying Code Object

START TRANSACTION;

CREATE TABLE rval(i integer,j integer);
INSERT INTO rval VALUES (1,4), (2,3), (3,2), (4,1);


CREATE FUNCTION pyapi14_create_functions() returns table(s string) language P
{
	def function_to_string(fun):
		import marshal, string
		return '@' + "".join('\\x' + x.encode('hex') for x in marshal.dumps(fun.__code__))

	def myfun1(a,b):
		return a * b

	def myfun2(a,b):
		return a * 20 + b

	def myfun3(a,b):
		import math
		return numpy.power(a,b)

	def myfun4(a,b):
		def mult(a,b):
			return a * b
		return mult(a,b)

	def myfun5(a,b):
		def mult(a,b):
			def timestwenty(a):
				return a * 20
			return timestwenty(a) * b
		return mult(a,b)

	_conn.execute('CREATE FUNCTION myfun1(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE PYTHON {%s};' % function_to_string(myfun1))
	_conn.execute('CREATE FUNCTION myfun2(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE PYTHON {%s};' % function_to_string(myfun2))
	_conn.execute('CREATE FUNCTION myfun3(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE PYTHON {%s};' % function_to_string(myfun3))
	_conn.execute('CREATE FUNCTION myfun4(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE PYTHON {%s};' % function_to_string(myfun4))
	_conn.execute('CREATE FUNCTION myfun5(a INTEGER, b INTEGER) RETURNS INTEGER LANGUAGE PYTHON {%s};' % function_to_string(myfun5))
	return [function_to_string(myfun1), 
			function_to_string(myfun2),
			function_to_string(myfun3),
			function_to_string(myfun4),
			function_to_string(myfun5)];
};

SELECT * FROM pyapi14_create_functions();

# note: when creating the SQL function the format is '{@<function_code>};', the first @ symbolizes to the parser that it is not a regular function but an encoded code object

# def myfun1(a,b):
#    return a * b
SELECT myfun1(i,j) FROM rval;
DROP FUNCTION myfun1;

# def myfun2(a,b):
#    return a * 20 + b
SELECT myfun2(i,j) FROM rval;
DROP FUNCTION myfun2;

#def myfun3(a,b):
#	import math
#	return numpy.power(a,b)
SELECT myfun3(i,j) FROM rval;
DROP FUNCTION myfun3;

#def myfun4(a,b):
#	def mult(a,b):
#	   return a * b
#	return mult(a,b)
SELECT myfun4(i,j) FROM rval;
DROP FUNCTION myfun4;

#def myfun5(a,b):
#	def mult(a,b):
#		def timestwenty(a):
#			return a * 20
#	    return timestwenty(a) * b
#	return mult(a,b)
SELECT myfun5(i,j) FROM rval;
DROP FUNCTION myfun5;

DROP TABLE rval;


ROLLBACK;
