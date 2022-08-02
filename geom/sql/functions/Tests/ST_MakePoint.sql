create table points_tbl(g geometry(point));
insert into points_tbl values (st_makepoint(10, 10));
insert into points_tbl values (st_point(20, 20));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(pointz));
insert into points_tbl values (st_makepoint(10, 10, 10));
insert into points_tbl values (st_makepoint(10, 20, 30));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(x double, y double, z double, m double);
insert into points_tbl values (10, 20, 30, 40), (100, 200, 300, 400), (1, 2, 3, 4);
select st_makepoint(x,y) as "XY" from points_tbl;
select st_makepoint(x,y,z) as "XYZ" from points_tbl;
select st_makepointm(x,y,m) as "XYM" from points_tbl;
select st_makepoint(x,y,z,m) as "XYZM" from points_tbl;
drop table points_tbl;

