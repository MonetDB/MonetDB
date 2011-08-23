CREATE ARRAY matrix (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0);
SELECT * FROM matrix;
-- this update is necessary because 'v FLOAT DEFAULT (x + y) * 100' is not implemented yet.
UPDATE matrix SET v = (x + y) * 100;
SELECT * FROM matrix;
-- This is wat we really want to test
INSERT INTO matrix SELECT x-1, y, v FROM matrix WHERE x > 2;
SELECT * FROM matrix;
DROP ARRAY matrix;

