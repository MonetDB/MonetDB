-- 1 Dimensional fixed array
CREATE ARRAY array1D(x TINYINT DIMENSION[4], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
SELECT * FROM array1D;
DROP ARRAY array1D;

-- relational equivalent 
CREATE TABLE array1D(x TINYINT CHECK(x >=0 and x < 4), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D values 
( 0, 1, 0),
( 1, 1, 0),
( 2, 1, 0),
( 3, 1, 0);
SELECT * FROM array1D;
DROP ARRAY array1D;

-- a 2 Dimensional fixed array
CREATE ARRAY array2D(x TINYINT DIMENSION[4], y BIGINT DIMENSION[4], v INTEGER DEFAULT 2);
SELECT * FROM array2D;
DROP ARRAY array2D;

-- relational equivalent 
CREATE TABLE array2D(x TINYINT CHECK(x >=0 and x < 4), y BIGINT CHECK( y>=0 and y<4), v INTEGER DEFAULT 2);
INSERT INTO array2D values 
( 0,	0,	2 ),
( 0,	1,	2 ),
( 0,	2,	2 ),
( 0,	3,	2 ),
( 1,	0,	2 ),
( 1,	1,	2 ),
( 1,	2,	2 ),
( 1,	3,	2 ),
( 2,	0,	2 ),
( 2,	1,	2 ),
( 2,	2,	2 ),
( 2,	3,	2 ),
( 3,	0,	2 ),
( 3,	1,	2 ),
( 3,	2,	2 ),
( 3,	3,	2 );

SELECT * FROM array2D;
DROP ARRAY array2D;

-- a 3D data cube
CREATE ARRAY array3D(x INT DIMENSION[3], y INT DIMENSION[3], z INT DIMENSION [3], v INT DEFAULT 3);
SELECT * FROM array3D;
DROP ARRAY array3D;

-- relational equivalent 
CREATE TABLE array3D(x INT CHECK(x>=0 AND x< 3), y INT CHECK(y>=0 AND y< 3), z INT CHECK(z>=0 and z <3), v INT DEFAULT 3);
INSERT INTO array3D values 
( 0,	0,	0, 3),
( 0,	0,	0, 3),
( 0,	0,	0, 3),
( 0,	1,	0, 3),
( 0,	1,	0, 3),
( 0,	1,	0, 3),
( 0,	2,	0, 3),
( 0,	2,	0, 3),
( 0,	2,	0, 3),
( 1,	0,	0, 3),
( 1,	0,	0, 3),
( 1,	0,	0, 3),
( 1,	1,	0, 3),
( 1,	1,	0, 3),
( 1,	1,	0, 3),
( 1,	2,	0, 3),
( 1,	2,	0, 3),
( 1,	2,	0, 3),
( 2,	0,	0, 3),
( 2,	0,	0, 3),
( 2,	0,	0, 3),
( 2,	1,	0, 3),
( 2,	1,	0, 3),
( 2,	1,	0, 3),
( 2,	2,	0, 3),
( 2,	2,	0, 3),
( 2,	2,	0, 3),
( 3,	0,	0, 3),
( 3,	0,	0, 3),
( 3,	0,	0, 3),
( 3,	1,	0, 3),
( 3,	1,	0, 3),
( 3,	1,	0, 3),
( 3,	2,	0, 3),
( 3,	2,	0, 3),
( 3,	2,	0, 3);

SELECT * FROM array3D;
DROP ARRAY array3D;


-- Semantic arrors
CREATE ARRAY arrayErr( x INT DIMENSION[-1]);
CREATE ARRAY arrayErr( x INT DIMENSION[0]);
