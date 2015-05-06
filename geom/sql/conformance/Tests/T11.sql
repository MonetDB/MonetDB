-- Conformance Item T11
SELECT ST_IsEmpty(centerline) FROM road_segments WHERE name = 'Route 5' AND aliases = 'Main Street';
