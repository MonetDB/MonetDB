CREATE ARRAY diagonal(x INTEGER DIMENSION[4], y INTEGER DIMENSION[4] CHECK(x = y), v FLOAT DEFAULT 0.0);
SELECT * FROM diagonal;

UPDATE diagonal SET v = x +y;
SELECT * FROM diagonal;

DROP ARRAY diagonal;

