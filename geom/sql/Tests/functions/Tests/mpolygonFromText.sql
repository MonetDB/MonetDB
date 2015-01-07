create table polygons_tbl(g geometry(multipolygon));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((20 20, 30 30, 40 40, 20 20),(200 200, 300 300, 400 400, 200 200)))', 4326));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((30 30, 40 40, 50 50, 30 30),(300 300, 400 400, 500 500, 300 300)))', 0));
insert into polygons_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)'));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(multipolygon, 4326));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((10 10, 20 20, 30 30, 10 10),(100 100, 200 200, 300 300, 100 100)))'));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((20 20, 30 30, 40 40, 20 20),(200 200, 300 300, 400 400, 200 200)))', 4326));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((30 30, 40 40, 50 50, 30 30),(300 300, 400 400, 500 500, 300 300)))', 0));
insert into polygons_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(multipolygonz));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((10 10 10, 20 20 20, 30 30 30, 10 10 10),(100 100 100, 200 200 200, 300 300 300, 100 100 100)))'));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((20 20 20, 30 30 30, 40 40 40, 20 20 20),(200 200 200, 300 300 300, 400 400 400, 200 200 200)))', 4326));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((30 30 30, 40 40 40, 50 50 50, 30 30 30),(300 300 300, 400 400 400, 500 500 500, 300 300 300)))', 0));
insert into polygons_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)'));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(multipolygonz, 4326));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((10 10 10, 20 20 20, 30 30 30, 10 10 10),(100 100 100, 200 200 200, 300 300 300, 100 100 100)))'));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((20 20 20, 30 30 30, 40 40 40, 20 20 20),(200 200 200, 300 300 300, 400 400 400, 200 200 200)))', 4326));
insert into polygons_tbl values (st_mpolyfromtext('multipolygon(((30 30 30, 40 40 40, 50 50 50, 30 30 30),(300 300 300, 400 400 400, 500 500 500, 300 300 300)))', 0));
insert into polygons_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from polygons_tbl;
drop table polygons_tbl;
