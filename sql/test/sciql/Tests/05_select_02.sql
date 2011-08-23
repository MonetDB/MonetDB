CREATE ARRAY matrix (x INT DIMENSION[4], y INT DIMENSION[4], v FLOAT DEFAULT 0.0);
CREATE TABLE tbl (i int, k int);
INSERT INTO T VALUES (2, 11), (2, 12), (3, 11), (3, 12);
SELECT [tbl.k], [y], v FROM matrix JOIN tbl ON matrix.x = tbl.i;
DROP ARRAY matrix;
DROP TABLE tbl;

