create table points_tbl(g geometry(point));
insert into points_tbl values (st_pointfromtext('point(0 10)'));
insert into points_tbl values (st_pointfromtext('point(0 20)', 4326));
insert into points_tbl values (st_pointfromtext('point(0 20)', 0));
insert into points_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)'));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(point, 4326));
insert into points_tbl values (st_pointfromtext('point(0 10)', 4326));
insert into points_tbl values (st_pointfromtext('point(0 20)'));
insert into points_tbl values (st_pointfromtext('point(0 20)', 0));
insert into points_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)', 4329));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(pointz));
insert into points_tbl values (st_pointfromtext('point(0 10 20)'));
insert into points_tbl values (st_pointfromtext('point(0 20 20)', 4326));
insert into points_tbl values (st_pointfromtext('point(0 20 20)', 0));
insert into points_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)'));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(pointz, 4326));
insert into points_tbl values (st_pointfromtext('point(0 10 20)', 4326));
insert into points_tbl values (st_pointfromtext('point(0 20 20)'));
insert into points_tbl values (st_pointfromtext('point(0 20 20)', 0));
insert into points_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from points_tbl;
drop table points_tbl;

