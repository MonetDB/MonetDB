create table geo (g geometry(multipoint, 4326));
insert into geo values (st_mpointfromtext('MULTIPOINT(10 10, 20 20, 30 30)', 4326));
insert into geo values (st_mpointfromtext('MULTIPOINT(10 10, 20 20, 10 10)', 4326));
select st_isvalid(g) from geo;
drop table geo;

select ST_IsValid(ST_GeomFromText('POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))'));

SELECT geom AS "GEOMETRY" FROM geometries WHERE id<14 AND ST_IsValid(geom);

