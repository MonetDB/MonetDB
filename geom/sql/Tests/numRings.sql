select st_numInteriorRings(geom) as interior, st_nRings(geom) as total from (select ST_polygonFromText('POLYGON((1 2, 3 4, 5 6, 1 2), (3 4, 5 6, 7 8, 3 4))')) as foo(geom);

select st_numInteriorRings(geom) as interior, st_nRings(geom) as total from (SELECT ST_MPolyFromText('MULTIPOLYGON(((1 2, 3 4, 5 6, 1 2),(10 20, 30 40, 50 60, 10 20), (100 200, 300 400, 500 600, 100 200)), ((1 2, 3 4, 5 6, 1 2)))')) as foo(geom); 

select st_numInteriorRings(geom) as interior from (select st_linefromtext('linestring(1 2 3, 4 5 6, 7 8 9)')) as foo(geom);
select st_nRings(geom) as total from (select st_linefromtext('linestring(1 2 3, 4 5 6, 7 8 9)')) as foo(geom);
