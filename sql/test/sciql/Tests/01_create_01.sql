-- use the [size] shortcut for integer type dimensions
CREATE ARRAY test_01_01_tinyint  (x TINYINT  DIMENSION[4], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_01_smallint (x SMALLINT DIMENSION[4], v FLOAT);
CREATE ARRAY test_01_01_int      (x INTEGER  DIMENSION[4], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_01_bigint   (x BIGINT   DIMENSION[4], v FLOAT DEFAULT NULL);

SELECT * FROM test_01_01_tinyint;
SELECT * FROM test_01_01_smallint;
SELECT * FROM test_01_01_int;
SELECT * FROM test_01_01_bigint;

DROP ARRAY test_01_01_tinyint;
DROP ARRAY test_01_01_smallint;
DROP ARRAY test_01_01_int;
DROP ARRAY test_01_01_bigint;

