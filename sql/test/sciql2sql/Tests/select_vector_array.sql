-- Test construction of arrays from a 1D  fixed array
CREATE ARRAY array1D(x INTEGER DIMENSION[7], v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO array1D values 
( 0, 1, 1),
( 1, 1, 2),
( 2, 2, 1),
( 3, 2, 5),
( 4, 3, 7),
( 5, 3, 7),
( 6, 4, 1);

-- relational equivalent 
CREATE TABLE vector(x INTEGER CHECK(x >=0 and x < 7), v INTEGER DEFAULT 1, w INTEGER DEFAULT 0);
INSERT INTO vector values 
( 0, 1, 1),
( 1, 1, 2),
( 2, 2, 1),
( 3, 2, 5),
( 4, 3, 7),
( 5, 3, 7),
( 6, 4, 1);

-- create an array again
SELECT [x], v+w FROM array1D;

-- relational equivalent
SELECT x, v+w FROM vector;

-- shifting the dimension
SELECT [x+2], v+w FROM array1D;

-- relational equivalent
SELECT x+2, v+w FROM vector;

-- extend array with constant y
SELECT [x],[0], v+w FROM array1D;

-- relational equivalent
SELECT x, 0, v+w FROM vector;

-- reduce the array
SELECT [v], x, w FROM array1D;

-- relational equivalent  should ensure uniqueness
-- and density of the new dimension
-- From an operational point of view, this is what happens
CREATE TEMPORARY ARRAY tmp( v INTEGER DIMENSION, x INTEGER, w INTEGER);
INSERT INTO tmp SELECT v,x,w FROM vector;
-- which arbitrary drops elements.
-- To mimick this all but one row of a group should be deleted.
CREATE FUNCTION ord() RETURNS TABLE (v integer, x integer, w integer)
BEGIN
	RETURN SELECT row_number() as id, v,x,w FROM vector ORDER BY v,x,w;
END;
SELECT v, min(id) FROM ord() GROUP BY v;
-- In a strongly typed setting, a coercion error should be raised when 
SELECT (SELECT count(*) FROM vector)  = (SELECT count(*)
	FROM vector
	GROUP BY v
	HAVING count(v) =1);

DROP ARRAY array1D;
DROP TABLE vector;
