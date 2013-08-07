--! CREATE ARRAY matrix (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);
--! SELECT * FROM matrix;

--! UPDATE matrix SET v = CASE WHEN x>y THEN x + y WHEN x<y THEN x - y ELSE 0 END;
--! SELECT * FROM matrix;

--! DROP ARRAY matrix;

CREATE TABLE matrix(x INT CHECK(x >=0 && x < 4), y INT CHECK( y>=0 and y<4), v FLOAT DEFAULT 3.7);
INSERT INTO matrix values 
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

UPDATE matrix SET v = CASE WHEN x>y THEN x + y WHEN x<y THEN x - y ELSE 0 END;
SELECT * FROM matrix;

DROP TABLE matrix;

