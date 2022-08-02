SELECT ST_CoordDim('LINESTRING(1 2 3, 1 3 4, 5 6 7, 8 9 10, 11 12 13)');
SELECT ST_CoordDim(ST_Point(1,2));

SELECT geom AS "GEOMETRY", ST_CoordDim(geom) AS "COORDS" FROM geometries;
