
CREATE TABLE lines (l LINESTRING);
INSERT INTO lines values ('LINESTRING(10 10, 20 20)');
INSERT INTO lines values ('LINESTRING(10 10, 20 20, 30 40)');
INSERT INTO lines values ('POINT(10 10)');
INSERT INTO lines values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10))');
INSERT INTO lines values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10), (15 15, 15 20, 10 15, 15 15))');
INSERT INTO lines values ('LINESTRING(10 10)');
INSERT INTO lines values ('LINESTRING(10)');
INSERT INTO lines values ('LINESTRING()');
INSERT INTO lines values ('LINESTRING');
INSERT INTO lines values ('');
INSERT INTO lines values (NULL);
SELECT * FROM lines;
DROP TABLE lines;
