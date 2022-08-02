SELECT ST_Contains(smallc, bigc) As smallcontainsbig 
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc, ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

SELECT ST_Contains(bigc,smallc) As bigcontainssmall 
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc, ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

SELECT ST_Contains(bigc, ST_Union(smallc, bigc)) as bigcontainsunion 
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc, ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

SELECT ST_Contains(bigc, ST_ExteriorRing(bigc)) As bigcontainsexterior 
FROM (SELECT ST_Buffer(ST_GeomFromText('POINT(1 2)'), 10) As smallc, ST_Buffer(ST_GeomFromText('POINT(1 2)'), 20) As bigc) As foo;

SELECT geom AS "GEOMETRY", ST_Contains(geom, 'POINT (15 15)') AS "CONTAINS" FROM geometries WHERE id<14;
