SELECT ST_NRings(ST_polygonFromText('POLYGON((1 2, 3 4, 5 6, 1 2), (3 4, 5 6, 7 8, 3 4))') ) AS "RINGS";
SELECT ST_NRings(ST_MPolyFromText('MULTIPOLYGON(((1 2, 3 4, 5 6, 1 2),(10 20, 30 40, 50 60, 10 20), (100 200, 300 400, 500 600, 100 200)), ((1 2, 3 4, 5 6, 1 2)))') ) AS "RINGS";
SELECT ST_NRings(st_linefromtext('linestring(1 2 3, 4 5 6, 7 8 9)')) AS "RINGS";


SELECT geom AS "GEOMETRY", ST_NRings(geom) AS "RING" FROM geometries WHERE 5<=id and id<=10 OR id=18 OR id=23 OR id=26 OR id=27;
