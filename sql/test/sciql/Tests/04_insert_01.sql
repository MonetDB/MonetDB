CREATE ARRAY matrix_0401 (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);
SELECT * FROM matrix_0401;
-- this update is necessary because 'v FLOAT DEFAULT (x + y) * 100' is not implemented yet.
UPDATE matrix_0401 SET v = (x + y) * 100;
SELECT * FROM matrix_0401;
-- This is wat we really want to test
INSERT INTO matrix_0401 SELECT x-1, y, v FROM matrix_0401 WHERE x > 2;
SELECT * FROM matrix_0401;
DROP ARRAY matrix_0401;

