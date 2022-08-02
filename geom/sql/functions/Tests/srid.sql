select st_srid(st_pointfromtext('point(0 0)', 4326));
select st_srid(st_linefromtext('linestring(0 0, 1 1, 2 2)'));

select st_srid(st_setsrid(st_polygonfromtext('polygon((100 100, 100 200, 200 200, 200 100, 100 100), (10 10, 10 20, 20 20, 20 10, 10 10))'), 4326));


create table geo (g geometry(pointz, 4326));
insert into geo values(ST_MakePoint(10, 20, 30));
insert into geo values(st_setsrid(ST_MakePoint(10, 20, 30), 4326));
select st_srid(g) as original_SRID, st_srid(st_setsrid(g, 3819)) as set_SRID, g from geo;
drop table geo;
