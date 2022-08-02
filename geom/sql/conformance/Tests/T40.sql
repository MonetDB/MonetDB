-- Conformance Item T40
SELECT ST_Within(boundary, footprint) FROM named_places, buildings WHERE named_places.name = 'Ashton' AND buildings.address = '215 Main Street';
