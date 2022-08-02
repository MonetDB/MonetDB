-- Conformance Item T34
SELECT ST_AsText(ST_Centroid(shores)) FROM ponds WHERE fid = 120;
