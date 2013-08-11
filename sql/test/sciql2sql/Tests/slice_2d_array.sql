-- a 2 Dimensional fixed array
CREATE ARRAY array2D(x TINYINT DIMENSION[4], y BIGINT DIMENSION[4], v INTEGER DEFAULT 2);
SELECT * FROM array2D;
DROP ARRAY array2D;

-- relational equivalent 
CREATE TABLE matrix(x TINYINT CHECK(x >=0 and x < 4), y BIGINT CHECK( y>=0 and y<4), v INTEGER DEFAULT 2);
INSERT INTO matrix values 
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

-- point slicing
SELECT * FROM array2D[0][0];
SELECT * FROM array2D[1][1];

-- relational
SELECT * FROM matrix WHERE x= 0 AND y = 0;
SELECT * FROM matrix WHERE x= 1 AND y = 1;

-- row and col based selection
SELECT * FROM array2D[0][*];
SELECT * FROM array2D[*][1];

-- relational
SELECT * FROM matrix WHERE x = 0;
SELECT * FROM matrix WHERE y = 1;

-- ideally we should use variables
DECLARE xval INTEGER;
DECLARE yval INTEGER;
SET xval =1;
SET yval =1;

SELECT * FROM array2D[xval][*];
SELECT * FROM array2D[*][yval];

-- same answer as before


-- extracting a chuck
SELECT * FROM array2D[xval:xval+2][yval:yval+3];

-- relational equivalent
SELECT * FROM matrix WHERE x>= xval AND x < xval+2 AND y >=yval AND y < yval +3;

DROP ARRAY array2D;
DROP TABLE matrix;


-- Semantic arrors
