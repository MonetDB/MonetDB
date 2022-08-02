-- Conformance Item T27
SELECT ST_AsText(ST_ExteriorRing(shore)) FROM lakes WHERE name = 'Blue Lake';
