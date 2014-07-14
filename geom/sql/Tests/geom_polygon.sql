
CREATE TABLE polygons (p POLYGON);
INSERT INTO polygons values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10))');
INSERT INTO polygons values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10), (15 15, 15 20, 10 15, 15 15))');
INSERT INTO polygons values ('LINESTRING(10 10, 20 20)');
INSERT INTO polygons values ('LINESTRING(10 10, 20 20, 30 40)');
INSERT INTO polygons values ('POLYGON((10 10, 10 20, 20 20, 20 15, 10 10))');
INSERT INTO polygons values ('POLYGON((10 10, 10 20, 20 20, 20 15))');
INSERT INTO polygons values ('POINT(10 10)');
INSERT INTO polygons values ('POLYGON((10 10))');
INSERT INTO polygons values ('POLYGON((10))');
INSERT INTO polygons values ('POLYGON(())');
INSERT INTO polygons values ('POLYGON()');
INSERT INTO polygons values ('POLYGON');
INSERT INTO polygons values ('');
INSERT INTO polygons values (NULL);
SELECT * FROM polygons;
DROP TABLE polygons;
