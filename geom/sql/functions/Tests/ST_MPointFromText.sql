create table points_tbl(g geometry(multipoint));
insert into points_tbl values (st_mpointfromtext('multipoint(10 10, 20 20)'));
insert into points_tbl values (st_mpointfromtext('multipoint(20 20, 30 30)', 4326));
insert into points_tbl values (st_mpointfromtext('multipoint(30 30, 40 40)', 0));
insert into points_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)'));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(multipoint, 4326));
insert into points_tbl values (st_mpointfromtext('multipoint(10 10, 20 20)'));
insert into points_tbl values (st_mpointfromtext('multipoint(20 20, 30 30)', 4326));
insert into points_tbl values (st_mpointfromtext('multipoint(30 30, 40 40)', 0));
insert into points_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(multipointz));
insert into points_tbl values (st_mpointfromtext('multipoint(10 10 10, 20 20 20)'));
insert into points_tbl values (st_mpointfromtext('multipoint(20 20 20, 30 30 30)', 4326));
insert into points_tbl values (st_mpointfromtext('multipoint(30 30 30, 40 40 40)', 0));
insert into points_tbl values (st_linefromtext('linestring(10 10 10, 20 20 20, 30 30 40)'));
select * from points_tbl;
drop table points_tbl;

create table points_tbl(g geometry(multipointz, 4326));
insert into points_tbl values (st_mpointfromtext('multipoint(10 10 10, 20 20 20)'));
insert into points_tbl values (st_mpointfromtext('multipoint(20 20 20, 30 30 30)', 4326));
insert into points_tbl values (st_mpointfromtext('multipoint(30 30 30, 40 40 40)', 0));
insert into points_tbl values (st_linefromtext('linestring(10 10 10, 20 20 20, 30 30 30)', 4326));
select * from points_tbl;
drop table points_tbl;

select st_mpointfromtext(geom) from geometriesTxt WHERE id=1;
select st_mpointfromtext(geom) from geometriesTxt WHERE id=2;
select st_mpointfromtext(geom) from geometriesTxt WHERE id=3;
select st_mpointfromtext(geom) from geometriesTxt WHERE id=4;
select st_mpointfromtext(geom) from geometriesTxt WHERE id=5;
select st_mpointfromtext(geom) from geometriesTxt WHERE id=6;
select st_mpointfromtext(geom) from geometriesTxt WHERE id=7;
select st_mpointfromtext(geom) from geometriesTxt WHERE id=8;
