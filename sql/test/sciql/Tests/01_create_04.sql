-- use the [start:stop] shortcut for int-typed dimensions
CREATE ARRAY test01_04_tinyint  (x TINYINT  DIMENSION[1:5], v FLOAT DEFAULT 3.7);
CREATE ARRAY test01_04_smallint (x SMALLINT DIMENSION[1:5], v FLOAT DEFAULT 3.7);
CREATE ARRAY test01_04_int      (x INTEGER  DIMENSION[1:5], v FLOAT DEFAULT 3.7);
CREATE ARRAY test01_04_bigint   (x BIGINT   DIMENSION[1:5], v FLOAT DEFAULT 3.7);
-- but not for non-int-typed dimensions, e.g.:
CREATE ARRAY test01_04_real     (x REAL     DIMENSION[1:5], v FLOAT DEFAULT 3.7);
CREATE ARRAY test01_04_double   (x DOUBLE   DIMENSION[1:5], v FLOAT DEFAULT 3.7);

SELECT * FROM test01_04_tinyint;
SELECT * FROM test01_04_smallint;
SELECT * FROM test01_04_int;
SELECT * FROM test01_04_bigint;
SELECT * FROM test01_04_real;
SELECT * FROM test01_04_double;

DROP ARRAY test01_04_tinyint;
DROP ARRAY test01_04_smallint;
DROP ARRAY test01_04_int;
DROP ARRAY test01_04_bigint;
DROP ARRAY test01_04_real;
DROP ARRAY test01_04_double;

