create table lines_tbl(g geometry(linestring));
insert into lines_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 30)'));
insert into lines_tbl values (st_linefromtext('linestring(20 20, 30 30, 40 40)', 4326));
insert into lines_tbl values (st_linefromtext('linestring(30 30, 40 40, 50 50)', 0));
insert into lines_tbl values (st_pointfromtext('point(0 0)'));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(linestring, 4326));
insert into lines_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 30)'));
insert into lines_tbl values (st_linefromtext('linestring(20 20, 30 30, 40 40)', 4326));
insert into lines_tbl values (st_linefromtext('linestring(30 30, 40 40, 50 50)', 0));
insert into lines_tbl values (st_pointfromtext('point(0 0)', 4326));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(linestringz));
insert into lines_tbl values (st_linefromtext('linestring(10 10 10, 20 20 20, 30 30 30)'));
insert into lines_tbl values (st_linefromtext('linestring(20 20 20, 30 30 30, 40 40 40)', 4326));
insert into lines_tbl values (st_linefromtext('linestring(30 30 30, 40 40 40, 50 50 50)', 0));
insert into lines_tbl values (st_pointfromtext('point(0 0 0)'));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(linestringz, 4326));
insert into lines_tbl values (st_linefromtext('linestring(10 10 10, 20 20 20, 30 30 30)'));
insert into lines_tbl values (st_linefromtext('linestring(20 20 20, 30 30 30, 40 40 40)', 4326));
insert into lines_tbl values (st_linefromtext('linestring(30 30 30, 40 40 40, 50 50 50)', 0));
insert into lines_tbl values (st_pointfromtext('point(0 0 0)', 4326));
select * from lines_tbl;
drop table lines_tbl;
