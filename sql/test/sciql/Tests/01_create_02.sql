-- does it work with other user defined schema's as well?

CREATE SCHEMA TS; 
SET SCHEMA TS; 

-- unbounded arrays
CREATE ARRAY test_01_02_tinyint  (x TINYINT  DIMENSION, v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_02_smallint (x SMALLINT DIMENSION, v FLOAT);
CREATE ARRAY test_01_02_int      (x INTEGER  DIMENSION, v FLOAT DEFAULT 3.7);
CREATE ARRAY test_01_02_bigint   (x BIGINT   DIMENSION, v FLOAT DEFAULT NULL);

SELECT * FROM test_01_02_tinyint;
SELECT * FROM test_01_02_smallint;
SELECT * FROM test_01_02_int;
SELECT * FROM test_01_02_bigint;

DROP ARRAY test_01_02_tinyint;
DROP ARRAY test_01_02_smallint;
DROP ARRAY test_01_02_int;
DROP ARRAY test_01_02_bigint;


SET SCHEMA SYS;
DROP SCHEMA TS; 

