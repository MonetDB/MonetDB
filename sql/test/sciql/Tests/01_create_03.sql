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

-- overflow checking
DECLARE vOverflow BIGINT;   SET vOverflow = 4294967296;

CREATE ARRAY test_01_03_tinyint_overflow       (x TINYINT  DIMENSION[vOverflow], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_smallint_overflow      (x SMALLINT DIMENSION[vOverflow], v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_03_int_overflow           (x INTEGER  DIMENSION[vOverflow], v FLOAT DEFAULT 3.7);

SELECT * FROM test_01_03_tinyint_overflow;
SELECT * FROM test_01_03_smallint_overflow;
SELECT * FROM test_01_03_int_overflow;

DROP ARRAY test_01_03_tinyint_overflow;
DROP ARRAY test_01_03_smallint_overflow;
DROP ARRAY test_01_03_int_overflow;

CREATE ARRAY test_01_03_tinyint_cell_overflow  (x SMALLINT DIMENSION[1], v TINYINT  DEFAULT 4294967296);
CREATE ARRAY test_01_03_smallint_cell_overflow (x SMALLINT DIMENSION[1], v SMALLINT DEFAULT 4294967296);
CREATE ARRAY test_01_03_int_cell_overflow      (x SMALLINT DIMENSION[1], v INT      DEFAULT 4294967296);

SELECT * FROM test_01_03_tinyint_cell_overflow;
SELECT * FROM test_01_03_smallint_cell_overflow;
SELECT * FROM test_01_03_int_cell_overflow;

DROP ARRAY test_01_03_tinyint_cell_overflow;
DROP ARRAY test_01_03_smallint_cell_overflow;
DROP ARRAY test_01_03_int_cell_overflow;

