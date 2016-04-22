-- Conformance Item T35
SELECT ST_Contains(shores, ST_PointOnSurface(shores)) FROM ponds WHERE fid = 120;
