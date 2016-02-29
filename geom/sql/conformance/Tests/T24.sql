-- Conformance Item T24
SELECT ST_AsText(ST_Centroid(boundary)) FROM named_places WHERE name = 'Goose Island';
