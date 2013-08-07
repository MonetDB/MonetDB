--! CREATE ARRAY matrix (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 3.3);

-- FIXME: do these queries update all 'v'-s in a row/column?
--! UPDATE matrix SET matrix[0:2].v = v * 1.19;
-- matrix[0:2] is wrong
-- UPDATE matrix SET v = v * 1.19 WHERE x[0:2];
-- slice x[0:2] excludes null?
--! SELECT * FROM matrix;

--! DROP ARRAY matrix;

CREATE TABLE matrix (x INT CHECK(x>=0 AND x <4), y INT CHECK(y>=0 AND y <4), v FLOAT DEFAULT 3.3);

-- FIXME: do these queries update all 'v'-s in a row/column?
UPDATE matrix SET v = v * 1.19 WHERE x >=0 AND x <2;
SELECT * FROM matrix;

DROP TABLE matrix;

