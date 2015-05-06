SELECT ST_IsClosed(st_linefromtext('LINESTRING(10 10, 20 20)'));
SELECT ST_IsClosed(st_linefromtext('LINESTRING(10 10, 20 20, 10 10)'));
SELECT ST_IsClosed(st_mlinefromtext('MULTILINESTRING((10 10, 20 20, 10 10),(10 10, 20 20))'));
SELECT ST_IsClosed(st_pointfromtext('POINT(10 10)'));
SELECT ST_IsClosed(st_mpointfromtext('MULTIPOINT((10 10), (20 20))'));
SELECT ST_IsClosed(st_polygonfromtext('POLYGON((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
SELECT ST_IsClosed(st_linefromtext('LINESTRING(10 10 10, 20 20 20, 10 10 10)'));

create table geo (g geometry(linestring, 4326));
insert into geo values (st_linefromtext('LINESTRING(10 10, 20 20)', 4326));
insert into geo values (st_linefromtext('LINESTRING(10 10, 20 20, 10 10)', 4326));
select st_isclosed(g) from geo;
drop table geo;

create table geo (g geometry(polygonz, 4326));
insert into geo values (st_polygonfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))', 4326));
select st_isclosed(g) from geo;
drop table geo;

SELECT geom AS "GEOMETRY", ST_IsClosed(geom) FROM geometries WHERE id<14;
