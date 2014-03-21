-- Conformance Item T39
SELECT Touches(centerline, shore) FROM streams, lakes WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
