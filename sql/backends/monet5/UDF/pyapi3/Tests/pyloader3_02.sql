
START TRANSACTION;

CREATE TABLE mytable4(a1 int, a2 int, a3 int, a4 int);
CREATE TABLE mytable3(a1 int, a2 int, a3 int);
CREATE TABLE mytable2(a1 int, a2 int);


CREATE LOADER myfunc(nvalues int, ncols int) LANGUAGE PYTHON {
	for i in range(nvalues):
		res = dict()
		for j in range(ncols):
			res['a'+str(j+1)] = (i+1)*(j+1)
		_emit.emit(res)
};

COPY LOADER INTO mytable3 FROM myfunc(10, 3);
COPY LOADER INTO mytable4 FROM myfunc(10, 3);
COPY LOADER INTO mytable2 FROM myfunc(20, 2);

SELECT * FROM mytable4;
SELECT * FROM mytable3;
SELECT * FROM mytable2;


DROP TABLE mytable3;
DROP TABLE mytable2;

DROP LOADER myfunc;


ROLLBACK;
