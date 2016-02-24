-- Conformance Item T18
SELECT ST_AsText(ST_EndPoint(centerline)) FROM road_segments WHERE fid = 102;
