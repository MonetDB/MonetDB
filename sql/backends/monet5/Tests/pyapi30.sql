
START TRANSACTION;

CREATE TABLE mytable(i DOUBLE, d STRING);

CREATE LOADER myfunc() LANGUAGE PYTHON {
	emit({'i':42,'d':'hello'})
};

SELECT * FROM functions WHERE name='myfunc';

EXPLAIN COPY INTO mytable FROM LOADER myfunc();

COPY INTO mytable FROM LOADER myfunc();

DROP TABLE mytable;
DROP LOADER myfunc;

ROLLBACK;
