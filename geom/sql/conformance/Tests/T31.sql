-- Conformance Item T31
SELECT ST_AsText(ST_GeometryN(centerlines, 2)) FROM divided_routes WHERE name = 'Route 75';
