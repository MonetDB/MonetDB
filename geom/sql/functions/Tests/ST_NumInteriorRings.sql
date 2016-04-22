SELECT ST_NumInteriorRings(ST_polygonFromText('POLYGON((1 2, 3 4, 5 6, 1 2), (3 4, 5 6, 7 8, 3 4))') ) AS "INTERIOR RINGS";
SELECT ST_NumInteriorRings(ST_MPolyFromText('MULTIPOLYGON(((1 2, 3 4, 5 6, 1 2),(10 20, 30 40, 50 60, 10 20), (100 200, 300 400, 500 600, 100 200)), ((1 2, 3 4, 5 6, 1 2)))') ) AS "INTERIOR RINGS";
SELECT ST_NumInteriorRings(st_linefromtext('linestring(1 2 3, 4 5 6, 7 8 9)')) AS "INTERIOR RINGS";


SELECT id, geom AS "GEOMETRY", ST_NumInteriorRings(geom) AS "INTERIOR RINGS" FROM geometries WHERE 5<=id AND id<=10 OR id=18 OR id=23 OR id=26 OR id=27;
