--! CREATE ARRAY diagonal(x INTEGER DIMENSION[4], y INTEGER DIMENSION[4] CHECK(x = y), v FLOAT DEFAULT 0.0);
--! SELECT * FROM diagonal;

--! UPDATE diagonal SET v = x +y;
--! SELECT * FROM diagonal;

DROP ARRAY diagonal;

CREATE TABLE diagonal(x INTEGER CHECK(x >=0 && x < 4), y INTEGER CHECK( y>=0 AND y<4 AND x = y), v FLOAT DEFAULT 0.0);
INSERT INTO diagonal values 
( 0,	0,	0.0	),
( 0,	1,	0.0	),
( 0,	2,	0.0	),
( 0,	3,	0.0	),
( 0,	4,	0.0	),
( 1,	0,	0.0	),
( 1,	1,	0.0	),
( 1,	2,	0.0	),
( 1,	3,	0.0	),
( 1,	4,	0.0	),
( 2,	0,	0.0	),
( 2,	1,	0.0	),
( 2,	2,	0.0	),
( 2,	3,	0.0	),
( 2,	4,	0.0	),
( 3,	0,	0.0	),
( 3,	1,	0.0	),
( 3,	2,	0.0	),
( 3,	3,	0.0	),
( 3,	4,	0.0	);

UPDATE diagonal SET v = x +y;
SELECT * FROM diagonal;

DROP TABLE diagonal;

