CREATE ARRAY matrix (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 3.3);

-- FIXME: do these queries update all 'v'-s in a row/column?
UPDATE matrix SET matrix[0:2].v = v * 1.19;
SELECT * FROM matrix;

DROP ARRAY matrix;

