SELECT ST_IsRing(st_linefromtext('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'));
SELECT ST_IsRing(st_linefromtext('LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)'));
select ST_IsRing(ST_GeomFromText('POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))'));

SELECT geom AS "GEOMETRY" FROM geometries WHERE id<14 AND ST_IsRing(geom);

