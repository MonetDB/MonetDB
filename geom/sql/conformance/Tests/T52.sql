-- Conformance Item T52
SELECT ST_AsText(ST_ConvexHull(shore)) FROM lakes WHERE lakes.name = 'Blue Lake';
