SELECT ST_AsBinary(ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))'));
SELECT ST_AsBinary(ST_GeomFromText('linestring(10 10,10 11,11 11)'));
SELECT ST_AsBinary(ST_GeomFromText('linearring(10 10,10 11,11 11,10 10)'));
