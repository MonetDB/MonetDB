create table lines_tbl(g geometry(multilinestring));
insert into lines_tbl values (st_mlinefromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
insert into lines_tbl values (st_mlinefromtext('multilinestring((20 20, 30 30, 40 40), (50 50, 60 60, 70 70))', 4326));
insert into lines_tbl values (st_mlinefromtext('multilinestring((30 30, 40 40, 50 50), (60 60, 70 70, 80 80))', 0));
insert into lines_tbl values (st_mlinefromtext('point(0 0)'));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(multilinestring, 4326));
insert into lines_tbl values (st_mlinefromtext('multilinestring((10 10, 20 20, 30 30), (40 40, 50 50, 60 60))'));
insert into lines_tbl values (st_mlinefromtext('multilinestring((20 20, 30 30, 40 40), (50 50, 60 60, 70 70))', 4326));
insert into lines_tbl values (st_mlinefromtext('multilinestring((30 30, 40 40, 50 50), (60 60, 70 70, 80 80))', 0));
insert into lines_tbl values (st_mlinefromtext('point(0 0)', 4326));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(multilinestringz));
insert into lines_tbl values (st_mlinefromtext('multilinestring((10 10 10, 20 20 20, 30 30 30), (40 40 40, 50 50 50, 60 60 60))'));
insert into lines_tbl values (st_mlinefromtext('multilinestring((20 20 20, 30 30 30, 40 40 40), (50 50 50, 60 60 60, 70 70 70))', 4326));
insert into lines_tbl values (st_mlinefromtext('multilinestring((30 30 30, 40 40 40, 50 50 50), (60 60 60, 70 70 70, 80 80 80))', 0));
insert into lines_tbl values (st_mlinefromtext('point(0 0)'));
select * from lines_tbl;
drop table lines_tbl;

create table lines_tbl(g geometry(multilinestringz, 4326));
insert into lines_tbl values (st_mlinefromtext('multilinestring((10 10 10, 20 20 20, 30 30 30), (40 40 40, 50 50 50, 60 60 60))'));
insert into lines_tbl values (st_mlinefromtext('multilinestring((20 20 20, 30 30 30, 40 40 40), (50 50 50, 60 60 60, 70 70 70))', 4326));
insert into lines_tbl values (st_mlinefromtext('multilinestring((30 30 30, 40 40 40, 50 50 50), (60 60 60, 70 70 70, 80 80 80))', 0));
insert into lines_tbl values (st_mlinefromtext('point(0 0)', 4326));
select * from lines_tbl;
drop table lines_tbl;

select st_mlinefromtext(geom) from geometriesTxt WHERE id=1;
select st_mlinefromtext(geom) from geometriesTxt WHERE id=2;
select st_mlinefromtext(geom) from geometriesTxt WHERE id=3;
select st_mlinefromtext(geom) from geometriesTxt WHERE id=4;
select st_mlinefromtext(geom) from geometriesTxt WHERE id=5;
select st_mlinefromtext(geom) from geometriesTxt WHERE id=6;
select st_mlinefromtext(geom) from geometriesTxt WHERE id=7;
select st_mlinefromtext(geom) from geometriesTxt WHERE id=8;
