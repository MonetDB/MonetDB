-- Conformance Item T17
SELECT ST_AsText(ST_StartPoint(centerline)) FROM road_segments WHERE fid = 102;
