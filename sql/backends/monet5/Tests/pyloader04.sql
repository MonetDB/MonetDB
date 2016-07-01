
# unmatched element (a3) present in dict, this should throw an error
START TRANSACTION;
CREATE TABLE pyloader04table(a1 int, a2 int);
CREATE LOADER pyloader04() LANGUAGE PYTHON {
	_emit.emit({'a1': 3, 'a2': 4, 'a3': 5})
};
COPY LOADER INTO pyloader04table FROM pyloader04();
ROLLBACK;

# use non-string type as key
START TRANSACTION;
CREATE TABLE pyloader04table(a1 int, a2 int);
CREATE LOADER pyloader04() LANGUAGE PYTHON {
	_emit.emit({'a1': 3, 'a2': 4, 3: 5})
};
COPY LOADER INTO pyloader04table FROM pyloader04();
ROLLBACK;

# return empty list
START TRANSACTION;
CREATE TABLE pyloader04table(a1 int, a2 int);
CREATE LOADER pyloader04() LANGUAGE PYTHON {
	_emit.emit({'a1': [], 'a2': numpy.array([])})
};
COPY LOADER INTO pyloader04table FROM pyloader04();
ROLLBACK;

# empty dictionary
START TRANSACTION;
CREATE TABLE pyloader04table(a1 int, a2 int);
CREATE LOADER pyloader04() LANGUAGE PYTHON {
	_emit.emit({})
};
COPY LOADER INTO pyloader04table FROM pyloader04();
ROLLBACK;

# unsupported python object
START TRANSACTION;
CREATE TABLE pyloader04table(a1 int, a2 int);
CREATE LOADER pyloader04() LANGUAGE PYTHON {
	class MyClass:
		i = 1234

	_emit.emit({'a1': MyClass()})
};
COPY LOADER INTO pyloader04table FROM pyloader04();
ROLLBACK;

# fail str -> int conversion
START TRANSACTION;
CREATE TABLE pyloader04table(a1 int, a2 int);
CREATE LOADER pyloader04() LANGUAGE PYTHON {
	_emit.emit({'a1': 'hello'})
};
COPY LOADER INTO pyloader04table FROM pyloader04();
ROLLBACK;

# test quoted names
START TRANSACTION;
CREATE TABLE pyloader04table("select" int, "from" int);
CREATE LOADER pyloader04() LANGUAGE PYTHON {
	_emit.emit({'select': 3, 'from': 4})
};
COPY LOADER INTO pyloader04table FROM pyloader04();
SELECT * FROM pyloader04table;
DROP TABLE pyloader04table;
DROP LOADER pyloader04;
ROLLBACK;
