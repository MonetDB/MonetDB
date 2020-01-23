-- Conformance Item T47
SELECT ST_AsText(ST_Intersection(centerline, shore)) FROM tstreams, lakes WHERE tstreams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
