-- Conformance Item T39
SELECT ST_Touches(centerline, shore) FROM tstreams, lakes WHERE tstreams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
