-- CREATE SCHEMA
CREATE TABLE geom(id integer, g geometry);
INSERT INTO geom VALUES(1, point(1,1));
INSERT INTO geom VALUES(2, point(2,2));
INSERT INTO geom VALUES(3, NULL);

-- CHECK FUNCTIONS (Each functions is used twice with normal arguments and with null arguments)
SELECT id, Area(g) FROM geom WHERE id < 3; 
SELECT id, Area(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, Length(g) FROM geom WHERE id < 3;
SELECT id, Length(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT Distance(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Distance(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT id, Buffer(g, 10) FROM geom WHERE id < 3;
SELECT id, Buffer(g, 10) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, ConvexHull(g) FROM geom WHERE id < 3;
SELECT id, ConvexHull(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT Intersection(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Intersection(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT "Union"(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT "Union"(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT Difference(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Difference(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT SymDifference(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT SymDifference(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

-- Test changed to accomodate differences between geos versions 3.2
-- and 3.3.  When geos 3.3 is installed everywhere, the test can be
-- reverted and the new output approved.  Also see basic.
-- SELECT id, Dimension(g) FROM geom WHERE id < 3;
SELECT id, CASE WHEN Dimension(g) IN (0,1,2) THEN 3 ELSE Dimension(g) END FROM geom WHERE id < 3;
SELECT id, Dimension(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, GeometryTypeId(g) FROM geom WHERE id < 3;
SELECT id, GeometryTypeId(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, SRID(g) FROM geom WHERE id < 3;
SELECT id, SRID(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, Envelope(g) FROM geom WHERE id < 3;
SELECT id, Envelope(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, IsEmpty(g) FROM geom WHERE id < 3;
SELECT id, IsEmpty(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, IsSimple(g) FROM geom WHERE id < 3;
SELECT id, IsSimple(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, Boundary(g) FROM geom WHERE id < 3;
SELECT id, Boundary(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT Equals(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Equals(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT Disjoint(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Disjoint(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT "Intersect"(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT "Intersect"(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT Touches(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Touches(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT Crosses(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Crosses(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT Within(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Within(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT Contains(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Contains(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT Overlaps(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 1 AND g2.id = 2;
SELECT Overlaps(g1.g, g2.g) FROM geom g1, geom g2 WHERE g1.id = 2 AND g2.id = 3; -- null argument, throws exception

SELECT id, X(g) FROM geom WHERE id < 3;
SELECT id, X(g) FROM geom WHERE id = 3; -- null argument, throws exception

SELECT id, Y(g) FROM geom WHERE id < 3;
SELECT id, Y(g) FROM geom WHERE id = 3; -- null argument, throws exception


-- DESTROY SCHEMA
DROP TABLE geom;
