CREATE ARRAY matrix3 (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);

-- this update is necessary because 'v FLOAT DEFAULT <expr>' is not implemented yet.
UPDATE matrix3 SET v = CASE WHEN x>y THEN x + y WHEN x<y THEN x - y ELSE 0 END;

SELECT v FROM matrix3[3][2];
SELECT v FROM matrix3[*][1:3]; 
SELECT v FROM matrix3[0:2:4][0:2:4];

DROP ARRAY matrix3;

