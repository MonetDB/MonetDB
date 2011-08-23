-- create an array with <check constraint definition> on one of its dimensions
CREATE ARRAY diagonal(x INTEGER DIMENSION[4], y INTEGER DIMENSION[4] CHECK(x = y), v FLOAT DEFAULT 0.0);
SELECT * FROM diagonal;
DROP ARRAY diagonal;

