select st_astext(st_pointfromtext('point(10 10)'));
select st_astext(st_pointfromtext('point(20 20)', 4326));
select st_astext(st_pointfromtext('point(10 10 10)'));
select st_astext(st_makepoint(10, 10));
select st_astext(st_point(20, 20));
select st_astext(st_makepoint(10, 10, 10));
select st_astext(st_linefromtext('linestring(10 10, 20 20, 30 30)'));
select st_astext(st_linefromtext('linestring(20 20, 30 30, 40 40)', 4326));
select st_astext(st_linefromtext('linestring(20 20 20, 30 30 30, 40 40 40)', 4326));
select st_astext(st_polygonfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
select st_astext(st_polygonfromtext('polygon((20 20, 30 30, 40 40, 20 20))', 4326));
select st_astext(st_polygonfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
select st_astext(st_mpointfromtext('multipoint(10 10, 20 20)'));
select st_astext(st_mpointfromtext('multipoint(20 20, 30 30)', 4326));
select st_astext(st_mpointfromtext('multipoint(20 20 20, 30 30 30)', 4326));
select st_astext(st_mlinefromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
select st_astext(st_mlinefromtext('multilinestring((20 20, 30 30, 40 40), (50 50, 60 60, 70 70))', 4326));
select st_astext(st_mlinefromtext('multilinestring((20 20 20, 30 30 30, 40 40 40), (50 50 50, 60 60 60, 70 70 70))', 4326));
select st_astext(st_mpolyfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));
select st_astext(st_mpolyfromtext('multipolygon(((20 20, 30 30, 40 40, 20 20),(200 200, 300 300, 400 400, 200 200)))', 4326));
select st_astext(st_mpolyfromtext('multipolygon(((10 10 10, 20 20 20, 30 30 30, 10 10 10),(100 100 100, 200 200 200, 300 300 300, 100 100 100)))'));

select st_astext(st_geomfromtext('point(10 10)'));
select st_astext(st_geomfromtext('linestring(10 10, 20 20, 30 30)'));
select st_astext(st_geomfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
select st_astext(st_geomfromtext('multipoint(10 10, 20 20)'));
select st_astext(st_geomfromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
select st_astext(st_geomfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));

create table points_tbl(g geometry(point));
insert into points_tbl values (st_pointfromtext('point(10 10)'));
select st_astext(g) from points_tbl;
drop table points_tbl;

create table lines_tbl(g geometry(linestring));
insert into lines_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 30)'));
select st_astext(g) from lines_tbl;
drop table lines_tbl;

create table polygons_tbl(g geometry(polygon));
insert into polygons_tbl values (st_polygonfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
select st_astext(g) from polygons_tbl;
drop table polygons_tbl;

create table points_tbl(g geometry(pointz));
insert into points_tbl values (st_pointfromtext('point(10 10 10)'));
select st_astext(g) from points_tbl;
drop table points_tbl;

create table lines_tbl(g geometry(linestringz));
insert into lines_tbl values (st_linefromtext('linestring(10 10 10, 20 20 20, 30 30 30)'));
select st_astext(g) from lines_tbl;
drop table lines_tbl;

create table polygons_tbl(g geometry(polygonz));
insert into polygons_tbl values (st_polygonfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
select st_astext(g) from polygons_tbl;
drop table polygons_tbl;
