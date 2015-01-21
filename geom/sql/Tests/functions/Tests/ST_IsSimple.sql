SELECT ST_IsSimple(ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))'));
SELECT ST_IsSimple(ST_GeomFromText('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)'));

create table geo (g geometry(polygon, 4326));
insert into geo values(ST_GeomFromText('POLYGON((1 2, 3 4, 5 6, 1 2))', 4326));
select st_IsSimple(g) from geo;
drop table geo;

create table geo (g geometry(linestring, 4326));
insert into geo values(ST_GeomFromText('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)', 4326));
select st_IsSimple(g) from geo;
drop table geo;

create table geo (g geometry(multipoint, 4326));
insert into geo values (st_mpointfromtext('multipoint(10 10, 20 20, 30 30)', 4326));
insert into geo values (st_mpointfromtext('multipoint(10 10, 20 20, 10 5)', 4326));
select st_isvalid(g) from geo;
drop table geo;

SELECT geom AS "GEOMETRY" FROM geometries WHERE id<11 AND ST_IsSimple(geom);
