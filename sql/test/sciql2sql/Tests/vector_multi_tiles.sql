-- Test tiling over a 1D  fixed array
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

-- simple projections using the FROM clause 
SELECT sum(v) FROM array1D[0];
SELECT sum(v) FROM array1D[0:3];

-- relational equivalences
SELECT sum(v) FROM vector WHERE x= 0;
SELECT sum(v) FROM vector WHERE x>= 0 AND x <3;

-- using a variable [SEGVAULT]
-- SELECT R.x, (SELECT sum(v) FROM array1D[R.x])
-- FROM array1D R;

-- relational equivalence
SELECT R.x, (SELECT sum(v) FROM vector WHERE x=R.x)
FROM vector R;

-- using a variable [SEGVAULT]
-- SELECT R.x, (SELECT sum(v) FROM array1D[0:R.v])
-- FROM array1D R;

-- relational equivalence
SELECT R.x, (SELECT sum(v) FROM vector WHERE x >= 0 AND x <R.x)
FROM vector R;

-- using a variable [SEGVAULT]
-- SELECT R.x, (SELECT sum(v) FROM array1D[0:2:R.v])
-- FROM array1D R;

-- relational equivalence
SELECT R.x, (SELECT sum(v) FROM vector WHERE x >= 0 AND x <R.x AND x % 2 = 0)
FROM vector R;

DROP ARRAY array1D;
DROP TABLE vector;

