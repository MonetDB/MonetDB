-- Conformance Item T29
SELECT ST_AsText(ST_InteriorRingN(shore, 1)) FROM lakes WHERE name = 'Blue Lake';
