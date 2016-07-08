-- Conformance Item T23
SELECT ST_AsText(ST_PointN(centerline, 1)) FROM road_segments WHERE fid = 102;
