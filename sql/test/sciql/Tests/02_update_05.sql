CREATE ARRAY matrix (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);

-- this update is necessary because 'v FLOAT DEFAULT <expr>' is not implemented yet.
UPDATE matrix SET v = CASE WHEN x>y THEN x + y WHEN x<y THEN x - y ELSE 0 END;
SELECT * FROM matrix;

UPDATE matrix SET matrix[x].v = CASE WHEN matrix[x].v < 2 THEN x WHEN matrix[x].v >3 THEN 10 * x ELSE 0 END;
SELECT * FROM matrix;

DROP ARRAY matrix;

