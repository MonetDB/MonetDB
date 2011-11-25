CREATE ARRAY matrix3 (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);

-- this update is necessary because 'v FLOAT DEFAULT <expr>' is not implemented yet.
UPDATE matrix3 SET v = CASE WHEN x>y THEN x + y WHEN x<y THEN x - y ELSE 0 END;
SELECT * FROM matrix3;

SELECT * FROM matrix3[3][2];
SELECT * FROM matrix3[*][2];
SELECT * FROM matrix3[3][*];
SELECT * FROM matrix3[*][*];

SELECT * FROM matrix3[2:4][1:3]; 
SELECT * FROM matrix3[2:*][1:3]; 
SELECT * FROM matrix3[2:4][*:3]; 

SELECT * FROM matrix3[0:2:4][1:1:3];
SELECT * FROM matrix3[0:2:4][*:1:3];
SELECT * FROM matrix3[0:*:4][1:1:3];
SELECT * FROM matrix3[0:2:4][1:1:*];

DROP ARRAY matrix3;

