-- Conformance Item T14
SELECT ST_AsText(ST_Envelope(boundary)) FROM named_places WHERE name = 'Goose Island';
