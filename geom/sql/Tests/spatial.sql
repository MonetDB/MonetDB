
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

SELECT * FROM geoms where g is NOT NULL;

SELECT Area(g), Length(g), Buffer(g, 2.0), ConvexHull(g) FROM geoms where g is not NULL;

SELECT a.g, b.g, Distance(a.g,b.g), Intersection(a.g,b.g), "Union"(a.g,b.g), Difference(a.g,b.g), SymDifference(a.g,b.g) FROM geoms a, geoms b where a.g is not NULL and b.g is not NULL order by a.g, b.g;

DROP TABLE geoms;
