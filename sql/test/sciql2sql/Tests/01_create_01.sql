-- use the [size] shortcut for integer type dimensions
--!CREATE ARRAY ary(x TINYINT DIMENSION[4], y BIGINT DIMENSION[-5], v FLOAT DEFAULT 3.7);
--!SELECT * FROM ary;
--!DROP ARRAY ary;

CREATE TABLE ary(x TINYINT CHECK(x >=0 && x < 4), y BIGINT CHECK( y>=-5 and y<0), v FLOAT DEFAULT 3.7);
INSERT INTO ary values 
( 0,	-4,	3.7	),
( 0,	-3,	3.7	),
( 0,	-2,	3.7	),
( 0,	-1,	3.7	),
( 0,	 0,	3.7	),
( 1,	-4,	3.7	),
( 1,	-3,	3.7	),
( 1,	-2,	3.7	),
( 1,	-1,	3.7	),
( 1,	 0,	3.7	),
( 2,	-4,	3.7	),
( 2,	-3,	3.7	),
( 2,	-2,	3.7	),
( 2,	-1,	3.7	),
( 2,	 0,	3.7	),
( 3,	-4,	3.7	),
( 3,	-3,	3.7	),
( 3,	-2,	3.7	),
( 3,	-1,	3.7	),
( 3,	 0,	3.7	);

SELECT * FROM ary;
DROP ARRAY ary;
