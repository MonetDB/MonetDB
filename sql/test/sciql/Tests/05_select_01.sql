CREATE ARRAY matrix (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);

-- this update is necessary because 'v FLOAT DEFAULT <expr>' is not implemented yet.
UPDATE matrix SET v = CASE WHEN x>y THEN x + y WHEN x<y THEN x - y ELSE 0 END;

SELECT x, y, v FROM matrix WHERE v >2; 
SELECT [x], [y], v FROM matrix WHERE v >2; 

DROP ARRAY matrix;

