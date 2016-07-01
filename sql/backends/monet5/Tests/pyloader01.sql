
START TRANSACTION;

CREATE TABLE mytable(a DOUBLE, d int, s STRING);

CREATE LOADER myfunc() LANGUAGE PYTHON {
	_emit.emit({'a':42,'d':1})
};

CREATE LOADER myfunc1(i integer) LANGUAGE PYTHON {
	_emit.emit({'a':i,'d':2})
};

CREATE LOADER myfunc2(i integer, f string) LANGUAGE PYTHON {
	_emit.emit({'a':i,'d':3})
};

CREATE LOADER myfunc3(i integer, f string, d double) LANGUAGE PYTHON {
	_emit.emit({'a':i,'d':4, 's': 'hello'})
};

SELECT name,func,mod,language,type,side_effect,varres,vararg FROM functions WHERE name='myfunc';


-- there is a reason for this, functions with 0, 1, 2 and 3+ arguments are handled differently.
COPY LOADER INTO mytable FROM myfunc3(46, 'asdf', 3.2);
COPY LOADER INTO mytable FROM myfunc2(45, 'asdf');
COPY LOADER INTO mytable FROM myfunc1(44);
COPY LOADER INTO mytable FROM myfunc();

SELECT * FROM mytable;

DROP TABLE mytable;
DROP ALL LOADER myfunc;

CREATE LOADER myfunc() LANGUAGE PYTHON {
};


DROP LOADER myfunc;
DROP LOADER myfunc1;
DROP LOADER myfunc2;
DROP LOADER myfunc3;

SELECT * FROM functions WHERE name='myfunc';


ROLLBACK;
