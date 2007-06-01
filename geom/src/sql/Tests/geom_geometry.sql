
CREATE TABLE geoms (g GEOMETRY);
INSERT INTO geoms values ('POINT(10 10)');
INSERT INTO geoms values ('LINESTRING(10 10, 20 20, 30 40)');
INSERT INTO geoms values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10))');
INSERT INTO geoms values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10), (15 15, 15 20, 10 15, 15 15))');
INSERT INTO geoms values ('LINESTRING(10 10)');
INSERT INTO geoms values ('LINESTRING(10)');
INSERT INTO geoms values ('LINESTRING()');
INSERT INTO geoms values ('LINESTRING');
INSERT INTO geoms values ('');
INSERT INTO geoms values ('POINT(10)');
INSERT INTO geoms values ('POINT()');
INSERT INTO geoms values ('POINT');
INSERT INTO geoms values ('');
INSERT INTO geoms values (NULL);
SELECT * FROM geoms;
DROP TABLE geoms;
