create table points_tbl(g geometry(point));
insert into points_tbl values (st_makepoint(10, 10));
insert into points_tbl values (st_point(20, 20));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(point, 4326));
insert into points_tbl values (st_makepoint(10, 10));
insert into points_tbl values (st_point(20, 20));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(pointz));
insert into points_tbl values (st_makepoint(10, 10, 10));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(pointz, 4326));
insert into points_tbl values (st_makepoint(10, 10, 10));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(linestring));
insert into points_tbl values (st_makepoint(10, 10));
insert into points_tbl values (st_point(20, 20));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(x double, y double);
insert into points_tbl values (10.0, 20.0);
insert into points_tbl values (20.0, 30.0);
select st_makepoint(x,y) from points_tbl;
drop table points_tbl;
