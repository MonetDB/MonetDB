select ST_GeometryN(ST_mpointfromtext('multipoint(1 2 7, 3 4 7, 5 6 7, 8 9 10)'), 4);
select ST_GeometryN(ST_mpointfromtext('multipoint(1 2 7, 3 4 7, 5 6 7, 8 9 10)'), 0);
select ST_GeometryN(ST_mpointfromtext('multipoint(1 2 7, 3 4 7, 5 6 7, 8 9 10)'), 1);

SELECT ST_GeometryN(
 ST_GeomCollFromText(
  'geometryCollection(
    polygon((2.5 2.5,4.5 2.5, 3.5 3.5, 2.5 2.5), (10 11, 12 11, 11 12, 10 11)), 
    multipoint(10 10, 20 20, 30 30))'), 
  4
);

SELECT ST_GeometryN(
 ST_GeomCollFromText(
  'geometryCollection(
    polygon((2.5 2.5,4.5 2.5, 3.5 3.5, 2.5 2.5), (10 11, 12 11, 11 12, 10 11)), 
    multipoint(10 10, 20 20, 30 30))'),
  2
);

SELECT ST_GeometryN(geom, 2) AS "GEOMETRY 2" FROM geometries WHERE id IN (19,20,21,22,23,24,25);

 

