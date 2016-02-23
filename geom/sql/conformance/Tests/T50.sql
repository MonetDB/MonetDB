-- Conformance Item T50
SELECT ST_AsText(ST_SymDifference(shore, boundary)) FROM lakes, named_places WHERE lakes.name = 'Blue Lake' AND named_places.name = 'Ashton';
