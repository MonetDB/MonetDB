SELECT ST_Boundary(ST_GeomFromText('LINESTRING(1 1,0 0, -1 1)'));

SELECT ST_Boundary(ST_GeomFromText('POLYGON((1 1,0 0, -1 1, 1 1))'));

SELECT ST_Boundary(ST_GeomFromText('POLYGON((1 1 1,0 0 1, -1 1 1, 1 1 1))'));

SELECT ST_Boundary(ST_GeomFromText('MULTILINESTRING((1 1 1,0 0 0.5, -1 1 1),(1 1 0.5,0 0 0.5, -1 1 0.5, 1 1 0.5) )'));

SELECT geom AS "GEOMETRY", ST_Boundary(geom) AS "BOUNDARY" FROM geometries where id<>12 and id<>13 and id<>24 and id<>25;
