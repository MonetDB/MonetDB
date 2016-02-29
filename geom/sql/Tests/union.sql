SELECT ST_Union(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(-2 3)') );
SELECT ST_Union(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)') );

create table points(p Geometry(POINT));
insert into points values(st_geomfromtext('point(10 20)'));
insert into points values(st_geomfromtext('point(20 30)'));
insert into points values(st_geomfromtext('point(30 40)'));
insert into points values(st_geomfromtext('point(40 50)'));

select st_union(p) from points;

drop table points;

SELECT st_union(the_geom) FROM
(SELECT ST_GeomFromText('POLYGON((-7 4.2,-7.1 4.2,-7.1 4.3, -7 4.2))') as the_geom
UNION ALL
SELECT ST_GeomFromText('POINT(5 5 5)') as the_geom
UNION ALL
SELECT ST_GeomFromText('POINT(-2 3 1)') as the_geom
UNION ALL
SELECT ST_GeomFromText('LINESTRING(5 5 5, 10 10 10)') as the_geom ) as foo;

SELECT ST_AsText(ST_Union(ST_GeomFromText('LINESTRING(1 2, 3 4)'), ST_GeomFromText('LINESTRING(3 4, 4 5)'))) As wkbunion;

