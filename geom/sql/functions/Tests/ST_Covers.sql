SELECT ST_Covers(smallc,smallc) As small_covers_small, 
	ST_Covers(smallc, bigc) As small_covers_big, 
	ST_Covers(bigc, ST_ExteriorRing(bigc)) As big_covers_exterior, 
	ST_Contains(bigc, ST_ExteriorRing(bigc)) As big_contains_exterior 
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc, ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

--it should return false but geos it seems only looks at the first polygon in the multipolygon and thus returns true
--but postgis also uses the geos for this function and returns false (why?)
SELECT ST_Covers('POLYGON((10 10,10 20,20 20,20 10,10 10))', 'MULTIPOLYGON(((10 10,10 20,20 20,20 10,10 10),(30 300,300 40,40 40,40 30,30 300)))');


SELECT geom AS "GEOMETRY", ST_Covers('POLYGON((20 20, 20 5, 5 5, 5 20, 20 20))', geom) FROM geometries;

--there is at least one more pair that should not because of the problem with the multipolygon against polygon.
--I am not sure whether the same happens when comparing whether a multipolygon is covered by a geometry collection. Postgis does not support geometry collections
SELECT g1.geom AS "GEOMETRY_1", g2.geom AS "GEOMETRY_2" FROM geometries g1, geometries g2 WHERE ST_Covers(g1.geom, g2.geom);

