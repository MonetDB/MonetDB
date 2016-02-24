-- Conformance Item T48
SELECT ST_AsText(ST_Difference(named_places.boundary, forests.boundary)) FROM named_places, forests WHERE named_places.name = 'Ashton' AND forests.name = 'Green Forest';
