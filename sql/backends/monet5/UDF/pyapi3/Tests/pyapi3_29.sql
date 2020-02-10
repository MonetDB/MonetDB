
START TRANSACTION;

CREATE TABLE mytable(i DOUBLE, d DOUBLE);
INSERT INTO mytable VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);

CREATE FUNCTION myfunc() RETURNS TABLE(n STRING) LANGUAGE PYTHON_MAP {
   res = _conn.execute("SELECT max(d) FROM mytable;")
   result = dict()
   result['n'] = str(res)
   return result
};

SELECT * FROM myfunc();

DROP TABLE mytable;
DROP FUNCTION myfunc;

ROLLBACK;
