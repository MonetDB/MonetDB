-- Conformance Item T25
SELECT ST_Contains(boundary, ST_PointOnSurface(boundary)) FROM named_places WHERE name = 'Goose Island';
