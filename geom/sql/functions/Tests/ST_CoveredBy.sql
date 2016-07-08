SELECT ST_CoveredBy(smallc,smallc) AS small_coveredBy_small, 
	ST_CoveredBy(smallc, bigc) AS small_coveredby_big, 
	ST_CoveredBy(bigc, smallc) as big_coveredby_small, 
	ST_CoveredBy(ST_ExteriorRing(bigc), bigc) as exterior_coveredby_big, 
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc, ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

--it should return false but geos it seems only looks ta the first polygon in the multipolygon and thus returns true
--but postgis also uses the geos for this function and returns false (why?)
SELECT ST_CoveredBy('MULTIPOLYGON(((10 10,10 20,20 20,20 10,10 10),(30 300,300 40,40 40,40 30,30 300)))', 'POLYGON((10 10,10 20,20 20,20 10,10 10))');

SELECT geom AS "GEOMETRY", ST_CoveredBy('POLYGON((10 10, 10 15, 15 15, 15 10, 10 10))', geom) FROM geometries;

--there is at least one more pair that should not because of the problem with the multipolygon against polygon.
--I am not sure whether the same happens when comparing whether a multipolygon is covered by a geometry collection. Postgis does not support geometry collections
SELECT g1.geom AS "GEOMETRY_1", g2.geom AS "GEOMETRY_2" FROM geometries g1, geometries g2 WHERE ST_CoveredBy(g1.geom, g2.geom);

