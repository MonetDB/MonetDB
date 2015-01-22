SELECT ST_Dimension(st_GeometryFromText('POINT(0 0)'));
SELECT ST_Dimension(st_GeometryFromText('LINESTRING(1 1,0 0)'));
SELECT ST_Dimension(st_GeometryFromText('polygon((0 0,1 0, 0 1, 1 1, 0 0))'));
SELECT ST_Dimension(st_GeomCollFromText('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))'));

SELECT DISTINCT ST_GeometryType(geom) AS "TYPE", ST_Dimension(geom) AS "DIM" FROM geometries;
