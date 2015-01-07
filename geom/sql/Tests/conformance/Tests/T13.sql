-- Conformance Item T13
SELECT ST_AsText(ST_Boundary(boundary)) FROM named_places WHERE name = 'Goose Island';
