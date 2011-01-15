
CREATE TABLE curves (c CURVE);
INSERT INTO curves values ('LINESTRING(10 10, 20 20)');
INSERT INTO curves values ('LINESTRING(10 10, 20 20, 30 40)');
INSERT INTO curves values ('POINT(10 10)');
INSERT INTO curves values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10))');
INSERT INTO curves values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10), (15 15, 15 20, 10 15, 15 15))');
INSERT INTO curves values ('LINESTRING(10 10)');
INSERT INTO curves values ('LINESTRING(10)');
INSERT INTO curves values ('LINESTRING()');
INSERT INTO curves values ('LINESTRING');
INSERT INTO curves values ('');
INSERT INTO curves values (NULL);
SELECT * FROM curves;
DROP TABLE curves;
