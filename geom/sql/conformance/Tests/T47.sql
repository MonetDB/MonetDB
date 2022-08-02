-- Conformance Item T47
SELECT ST_AsText(ST_Intersection(centerline, shore)) FROM streams, lakes WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
