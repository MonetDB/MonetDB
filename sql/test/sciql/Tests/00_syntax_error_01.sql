-- arrays must have at least one dimension
CREATE ARRAY test_00_01          (payload FLOAT);

-- the [size] shortcut may not be negative
CREATE ARRAY test_00_01_tinyint  (x TINYINT  DIMENSION[-4]);
CREATE ARRAY test_00_01_smallint (x SMALLINT DIMENSION[-4]);
CREATE ARRAY test_00_01_int      (x INTEGER  DIMENSION[-4]);
CREATE ARRAY test_00_01_bigint   (x BIGINT   DIMENSION[-4]);

