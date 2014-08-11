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

SELECT a.g, b.g, Equals(a.g,b.g), Disjoint(a.g,b.g), "Intersect"(a.g,b.g), Touches(a.g,b.g) FROM geoms a, geoms b where a.g is not NULL and b.g is not NULL order by a.g, b.g;

SELECT a.g, b.g, Crosses(a.g,b.g), Within(a.g,b.g), Contains(a.g,b.g), Overlaps(a.g,b.g) FROM geoms a, geoms b where a.g is not NULL and b.g is not NULL order by a.g, b.g;

SELECT a.g, b.g, Relate(a.g,b.g,'Touches') FROM geoms a, geoms b where a.g is not NULL and b.g is not NULL order by a.g, b.g;

SELECT a.g, b.g, Relate(a.g,b.g,'*F**T**F*') FROM geoms a, geoms b where a.g is not NULL and b.g is not NULL order by a.g, b.g;

DROP TABLE geoms;
