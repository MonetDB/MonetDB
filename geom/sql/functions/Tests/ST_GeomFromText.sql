create table points_tbl(g geometry(point));
insert into points_tbl values (st_geomfromtext('point(0 10)'));
insert into points_tbl values (st_geomfromtext('point(0 20)', 4326));
insert into points_tbl values (st_geomfromtext('point(0 20)', 0));
insert into points_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)'));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(point, 4326));
insert into points_tbl values (st_geomfromtext('point(0 10)', 4326));
insert into points_tbl values (st_geomfromtext('point(0 20)'));
insert into points_tbl values (st_geomfromtext('point(0 20)', 0));
insert into points_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)', 4329));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(pointz));
insert into points_tbl values (st_geomfromtext('point(0 10 20)'));
insert into points_tbl values (st_geomfromtext('point(0 20 20)', 4326));
insert into points_tbl values (st_geomfromtext('point(0 20 20)', 0));
insert into points_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)'));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(pointz, 4326));
insert into points_tbl values (st_geomfromtext('point(0 10 20)', 4326));
insert into points_tbl values (st_geomfromtext('point(0 20 20)'));
insert into points_tbl values (st_geomfromtext('point(0 20 20)', 0));
insert into points_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from points_tbl;
drop table points_tbl;

create table lines_tbl(g geometry(linestring));
insert into lines_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 30)'));
insert into lines_tbl values (st_geomfromtext('linestring(20 20, 30 30, 40 40)', 4326));
insert into lines_tbl values (st_geomfromtext('linestring(30 30, 40 40, 50 50)', 0));
insert into lines_tbl values (st_geomfromtext('point(0 0)'));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(linestring, 4326));
insert into lines_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 30)'));
insert into lines_tbl values (st_geomfromtext('linestring(20 20, 30 30, 40 40)', 4326));
insert into lines_tbl values (st_geomfromtext('linestring(30 30, 40 40, 50 50)', 0));
insert into lines_tbl values (st_geomfromtext('point(0 0)', 4326));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(linestringz));
insert into lines_tbl values (st_geomfromtext('linestring(10 10 10, 20 20 20, 30 30 30)'));
insert into lines_tbl values (st_geomfromtext('linestring(20 20 20, 30 30 30, 40 40 40)', 4326));
insert into lines_tbl values (st_geomfromtext('linestring(30 30 30, 40 40 40, 50 50 50)', 0));
insert into lines_tbl values (st_geomfromtext('point(0 0 0)'));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(linestringz, 4326));
insert into lines_tbl values (st_geomfromtext('linestring(10 10 10, 20 20 20, 30 30 30)'));
insert into lines_tbl values (st_geomfromtext('linestring(20 20 20, 30 30 30, 40 40 40)', 4326));
insert into lines_tbl values (st_geomfromtext('linestring(30 30 30, 40 40 40, 50 50 50)', 0));
insert into lines_tbl values (st_geomfromtext('point(0 0 0)', 4326));
select * from lines_tbl;
drop table lines_tbl;

create table polygons_tbl(g geometry(polygon));
insert into polygons_tbl values (st_geomfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
insert into polygons_tbl values (st_geomfromtext('polygon((20 20, 30 30, 40 40, 20 20))', 4326));
insert into polygons_tbl values (st_geomfromtext('polygon((30 30, 40 40, 50 50, 30 30))', 0));
insert into polygons_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)'));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(polygon, 4326));
insert into polygons_tbl values (st_geomfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
insert into polygons_tbl values (st_geomfromtext('polygon((20 20, 30 30, 40 40, 20 20))', 4326));
insert into polygons_tbl values (st_geomfromtext('polygon((30 30, 40 40, 50 50, 30 30))', 0));
insert into polygons_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(polygonz));
insert into polygons_tbl values (st_geomfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
insert into polygons_tbl values (st_geomfromtext('polygon((20 20 20, 30 30 30, 40 40 40, 20 20 20))', 4326));
insert into polygons_tbl values (st_geomfromtext('polygon((30 30 30, 40 40 40, 50 50 50, 30 30 30))', 0));
insert into polygons_tbl values (st_geomfromtext('linestring(10 10 10, 20 20 20, 30 30 30)'));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(polygonz, 4326));
insert into polygons_tbl values (st_geomfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
insert into polygons_tbl values (st_geomfromtext('polygon((20 20 20, 30 30 30, 40 40 40, 20 20 20))', 4326));
insert into polygons_tbl values (st_geomfromtext('polygon((30 30 30, 40 40 40, 50 50 50, 30 30 30))', 0));
insert into polygons_tbl values (st_geomfromtext('linestring(10 10 10, 20 20 20, 30 30 30)', 4326));
select * from polygons_tbl;
drop table polygons_tbl;

create table points_tbl(g geometry(multipoint));
insert into points_tbl values (st_geomfromtext('multipoint(10 10, 20 20)'));
insert into points_tbl values (st_geomfromtext('multipoint(20 20, 30 30)', 4326));
insert into points_tbl values (st_geomfromtext('multipoint(30 30, 40 40)', 0));
insert into points_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)'));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(multipoint, 4326));
insert into points_tbl values (st_geomfromtext('multipoint(10 10, 20 20)'));
insert into points_tbl values (st_geomfromtext('multipoint(20 20, 30 30)', 4326));
insert into points_tbl values (st_geomfromtext('multipoint(30 30, 40 40)', 0));
insert into points_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(multipointz));
insert into points_tbl values (st_geomfromtext('multipoint(10 10 10, 20 20 20)'));
insert into points_tbl values (st_geomfromtext('multipoint(20 20 20, 30 30 30)', 4326));
insert into points_tbl values (st_geomfromtext('multipoint(30 30 30, 40 40 40)', 0));
insert into points_tbl values (st_geomfromtext('linestring(10 10 10, 20 20 20, 30 30 40)'));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(multipointz, 4326));
insert into points_tbl values (st_geomfromtext('multipoint(10 10 10, 20 20 20)'));
insert into points_tbl values (st_geomfromtext('multipoint(20 20 20, 30 30 30)', 4326));
insert into points_tbl values (st_geomfromtext('multipoint(30 30 30, 40 40 40)', 0));
insert into points_tbl values (st_geomfromtext('linestring(10 10 10, 20 20 20, 30 30 30)', 4326));
select * from points_tbl;
drop table points_tbl;

create table lines_tbl(g geometry(multilinestring));
insert into lines_tbl values (st_geomfromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
insert into lines_tbl values (st_geomfromtext('multilinestring((20 20, 30 30, 40 40), (50 50, 60 60, 70 70))', 4326));
insert into lines_tbl values (st_geomfromtext('multilinestring((30 30, 40 40, 50 50), (60 60, 70 70, 80 80))', 0));
insert into lines_tbl values (st_geomfromtext('point(0 0)'));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(multilinestring, 4326));
insert into lines_tbl values (st_geomfromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
insert into lines_tbl values (st_geomfromtext('multilinestring((20 20, 30 30, 40 40), (50 50, 60 60, 70 70))', 4326));
insert into lines_tbl values (st_geomfromtext('multilinestring((30 30, 40 40, 50 50), (60 60, 70 70, 80 80))', 0));
insert into lines_tbl values (st_geomfromtext('point(0 0)', 4326));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(multilinestringz));
insert into lines_tbl values (st_geomfromtext('multilinestring((10 10 10, 20 20 20, 30 30 30), (40 40 40, 50 50 50, 60 60 60))'));
insert into lines_tbl values (st_geomfromtext('multilinestring((20 20 20, 30 30 30, 40 40 40), (50 50 50, 60 60 60, 70 70 70))', 4326));
insert into lines_tbl values (st_geomfromtext('multilinestring((30 30 30, 40 40 40, 50 50 50), (60 60 60, 70 70 70, 80 80 80))', 0));
insert into lines_tbl values (st_geomfromtext('point(0 0)'));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(multilinestringz, 4326));
insert into lines_tbl values (st_geomfromtext('multilinestring((10 10 10, 20 20 20, 30 30 30), (40 40 40, 50 50 50, 60 60 60))'));
insert into lines_tbl values (st_geomfromtext('multilinestring((20 20 20, 30 30 30, 40 40 40), (50 50 50, 60 60 60, 70 70 70))', 4326));
insert into lines_tbl values (st_geomfromtext('multilinestring((30 30 30, 40 40 40, 50 50 50), (60 60 60, 70 70 70, 80 80 80))', 0));
insert into lines_tbl values (st_geomfromtext('point(0 0)', 4326));
select * from lines_tbl;
drop table lines_tbl;

create table polygons_tbl(g geometry(multipolygon));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((20 20, 30 30, 40 40, 20 20),(200 200, 300 300, 400 400, 200 200)))', 4326));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((30 30, 40 40, 50 50, 30 30),(300 300, 400 400, 500 500, 300 300)))', 0));
insert into polygons_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)'));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(multipolygon, 4326));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((20 20, 30 30, 40 40, 20 20),(200 200, 300 300, 400 400, 200 200)))', 4326));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((30 30, 40 40, 50 50, 30 30),(300 300, 400 400, 500 500, 300 300)))', 0));
insert into polygons_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(multipolygonz));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((10 10 10, 20 20 20, 30 30 30, 10 10 10),(100 100 100, 200 200 200, 300 300 300, 100 100 100)))'));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((20 20 20, 30 30 30, 40 40 40, 20 20 20),(200 200 200, 300 300 300, 400 400 400, 200 200 200)))', 4326));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((30 30 30, 40 40 40, 50 50 50, 30 30 30),(300 300 300, 400 400 400, 500 500 500, 300 300 300)))', 0));
insert into polygons_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)'));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(multipolygonz, 4326));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((10 10 10, 20 20 20, 30 30 30, 10 10 10),(100 100 100, 200 200 200, 300 300 300, 100 100 100)))'));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((20 20 20, 30 30 30, 40 40 40, 20 20 20),(200 200 200, 300 300 300, 400 400 400, 200 200 200)))', 4326));
insert into polygons_tbl values (st_geomfromtext('multipolygon(((30 30 30, 40 40 40, 50 50 50, 30 30 30),(300 300 300, 400 400 400, 500 500 500, 300 300 300)))', 0));
insert into polygons_tbl values (st_geomfromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from polygons_tbl;
drop table polygons_tbl;

select st_geomfromtext(geom) from geometriesTxt;
