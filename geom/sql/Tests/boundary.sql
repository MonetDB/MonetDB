SELECT ST_Boundary(ST_GeomFromText('LINESTRING(1 1,0 0, -1 1)'));

SELECT ST_Boundary(ST_GeomFromText('POLYGON((1 1,0 0, -1 1, 1 1))'));

SELECT ST_Boundary(ST_GeomFromText('POLYGON((1 1 1,0 0 1, -1 1 1, 1 1 1))'));

SELECT ST_Boundary(ST_GeomFromText('MULTILINESTRING((1 1 1,0 0 0.5, -1 1 1),(1 1 0.5,0 0 0.5, -1 1 0.5, 1 1 0.5) )'));

