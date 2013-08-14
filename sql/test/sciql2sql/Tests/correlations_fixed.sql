-- Test manipulation of tiles over multple arrays
CREATE ARRAY array1(x INTEGER DIMENSION[7], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1 values 
( 0, 1, 1),
( 1, 1, 2),
( 2, 2, 1),
( 3, 2, 5),
( 4, 3, 7),
( 5, 3, 7),
( 6, 4, 1);

CREATE ARRAY array2(LIKE array1);
INSERT INTO array2 SELECT * FROM array1;

-- relational equivalent 
CREATE TABLE vector1(x INTEGER CHECK(x >=0 and x < 7), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO vector1 values 
( 0, 1, 1),
( 1, 1, 2),
( 2, 2, 1),
( 3, 2, 5),
( 4, 3, 7),
( 5, 3, 7),
( 6, 4, 1);
CREATE TABLE vector2(LIKE vector1);
INSERT INTO vector2 SELECT * FROM vector1;

-- work with joined arrays
SELECT array1.x, array2.x FROM array1[x] JOIN array2[x];

--relational equivalent
SELECT vector1.x, vector2.x FROM vector1 JOIN vector2 ON vector1.x = vector2.x;

-- use constant projections
SELECT array1.x, array2.x FROM array1[0] JOIN array2[1];

--relational equivalent
SELECT vector1.x, vector2.x FROM vector1 JOIN vector2 ON vector2.x = 1 AND vector1.x = 0;

-- use shifting
SELECT array1.x, array2.x FROM array1[x] JOIN array2[x+1];

--relational equivalent
SELECT vector1.x, vector2.x FROM vector1 JOIN vector2 ON vector2.x = vector1.x+1;

-- extend it to a thetajoin
SELECT array1.x, array2.x, sum(array1.v) FROM array1[x:x+2], array2[x:x+5];
SELECT array1.x, array2.x, sum(array1.v) FROM array1 A, array2 B GROUP BY A[x:x+2], B[x:x+5];

--relational equivalent

DROP ARRAY array1;
DROP ARRAY array2;
DROP TABLE vector1;
DROP TABLE vector2;

