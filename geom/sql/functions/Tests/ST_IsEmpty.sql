SELECT ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'));
SELECT ST_IsEmpty(ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))'));
SELECT ST_IsEmpty(ST_GeomFromText('LINESTRING EMPTY'));

SELECT geom AS "GEOMETRY" FROM geometries WHERE ST_IsEmpty(geom);
