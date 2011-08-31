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

-- Test changed to accomodate differences between geos versions 3.2
-- and 3.3.  When geos 3.3 is installed everywhere, the test can be
-- reverted and the new output approved.  Also see geom-null-tests.
-- SELECT Dimension(g), GeometryTypeId(g), SRID(g), Envelope(g) FROM geoms where g is not NULL;
SELECT CASE WHEN Dimension(g) IN (0,1,2) THEN 3 ELSE Dimension(g) END, GeometryTypeId(g), SRID(g), Envelope(g) FROM geoms where g is not NULL;

SELECT IsEmpty(g), IsSimple(g), Boundary(g) FROM geoms where g is not NULL;

DROP TABLE geoms;
