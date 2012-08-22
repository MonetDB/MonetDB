-- use the [size] shortcut for integer type dimensions
CREATE ARRAY test_01_03_tinyint  (x TINYINT  DIMENSION[4], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_smallint (x SMALLINT DIMENSION[4], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_int      (x INTEGER  DIMENSION[4], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_bigint   (x BIGINT   DIMENSION[4], v FLOAT DEFAULT 3.7);

SELECT * FROM test_01_03_tinyint;
SELECT * FROM test_01_03_smallint;
SELECT * FROM test_01_03_int;
SELECT * FROM test_01_03_bigint;

DROP ARRAY test_01_03_tinyint;
DROP ARRAY test_01_03_smallint;
DROP ARRAY test_01_03_int;
DROP ARRAY test_01_03_bigint;

-- use the [-size] shortcut for integer type dimensions
CREATE ARRAY test_01_03_neg_tinyint  (x TINYINT  DIMENSION[-4], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_neg_smallint (x SMALLINT DIMENSION[-4], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_neg_int      (x INTEGER  DIMENSION[-4], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_neg_bigint   (x BIGINT   DIMENSION[-4], v FLOAT DEFAULT 3.7);

SELECT * FROM test_01_03_neg_tinyint;
SELECT * FROM test_01_03_neg_smallint;
SELECT * FROM test_01_03_neg_int;
SELECT * FROM test_01_03_neg_bigint;

DROP ARRAY test_01_03_neg_tinyint;
DROP ARRAY test_01_03_neg_smallint;
DROP ARRAY test_01_03_neg_int;
DROP ARRAY test_01_03_neg_bigint;

-- use the [varname] shortcut for integer type dimensions
DECLARE vTinyint  TINYINT;  SET vTinyint = 6;
DECLARE vSmallint SMALLINT; SET vSmallint = 6;
DECLARE vInt      INTEGER;  SET vInt = 6;
DECLARE vBigint   BIGINT;   SET vBigint = 6;

CREATE ARRAY test_01_03_tinyint_tinyint   (x TINYINT  DIMENSION[vTinyint],  v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_smallint_tinyint  (x SMALLINT DIMENSION[vTinyint],  v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_int_tinyint       (x INTEGER  DIMENSION[vTinyint],  v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_bigint_tinyint    (x BIGINT   DIMENSION[vTinyint],  v FLOAT DEFAULT 3.7);

CREATE ARRAY test_01_03_tinyint_smallint  (x TINYINT  DIMENSION[vSmallint], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_smallint_smallint (x SMALLINT DIMENSION[vSmallint], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_int_smallint      (x INTEGER  DIMENSION[vSmallint], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_bigint_smallint   (x BIGINT   DIMENSION[vSmallint], v FLOAT DEFAULT 3.7);

CREATE ARRAY test_01_03_tinyint_int       (x TINYINT  DIMENSION[vInt],      v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_smallint_int      (x SMALLINT DIMENSION[vInt],      v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_int_int           (x INTEGER  DIMENSION[vInt],      v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_bigint_int        (x BIGINT   DIMENSION[vInt],      v FLOAT DEFAULT 3.7);

CREATE ARRAY test_01_03_tinyint_bigint    (x TINYINT  DIMENSION[vBigint],   v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_smallint_bigint   (x SMALLINT DIMENSION[vBigint],   v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_int_bigint        (x INTEGER  DIMENSION[vBigint],   v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_bigint_bigint     (x BIGINT   DIMENSION[vBigint],   v FLOAT DEFAULT 3.7);

SELECT * FROM test_01_03_tinyint_tinyint;
SELECT * FROM test_01_03_smallint_tinyint;
SELECT * FROM test_01_03_int_tinyint;
SELECT * FROM test_01_03_bigint_tinyint;

SELECT * FROM test_01_03_tinyint_smallint;
SELECT * FROM test_01_03_smallint_smallint;
SELECT * FROM test_01_03_int_smallint;
SELECT * FROM test_01_03_bigint_smallint;

SELECT * FROM test_01_03_tinyint_int;
SELECT * FROM test_01_03_smallint_int;
SELECT * FROM test_01_03_int_int;
SELECT * FROM test_01_03_bigint_int;

SELECT * FROM test_01_03_tinyint_bigint;
SELECT * FROM test_01_03_smallint_bigint;
SELECT * FROM test_01_03_int_bigint;
SELECT * FROM test_01_03_bigint_bigint;

DROP ARRAY test_01_03_tinyint_tinyint;
DROP ARRAY test_01_03_smallint_tinyint;
DROP ARRAY test_01_03_int_tinyint;
DROP ARRAY test_01_03_bigint_tinyint;

DROP ARRAY test_01_03_tinyint_smallint;
DROP ARRAY test_01_03_smallint_smallint;
DROP ARRAY test_01_03_int_smallint;
DROP ARRAY test_01_03_bigint_smallint;

DROP ARRAY test_01_03_tinyint_int;
DROP ARRAY test_01_03_smallint_int;
DROP ARRAY test_01_03_int_int;
DROP ARRAY test_01_03_bigint_int;

DROP ARRAY test_01_03_tinyint_bigint;
DROP ARRAY test_01_03_smallint_bigint;
DROP ARRAY test_01_03_int_bigint;
DROP ARRAY test_01_03_bigint_bigint;

