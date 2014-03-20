-- Conformance Item T47
SELECT AsText(Intersection(centerline, shore)) FROM streams, lakes WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
