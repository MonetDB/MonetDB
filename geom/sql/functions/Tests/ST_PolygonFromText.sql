create table polygons_tbl(g geometry(polygon));
insert into polygons_tbl values (st_polygonfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
insert into polygons_tbl values (st_polygonfromtext('polygon((20 20, 30 30, 40 40, 20 20))', 4326));
insert into polygons_tbl values (st_polygonfromtext('polygon((30 30, 40 40, 50 50, 30 30))', 0));
insert into polygons_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)'));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(polygon, 4326));
insert into polygons_tbl values (st_polygonfromtext('polygon((10 10, 20 20, 30 30, 10 10))'));
insert into polygons_tbl values (st_polygonfromtext('polygon((20 20, 30 30, 40 40, 20 20))', 4326));
insert into polygons_tbl values (st_polygonfromtext('polygon((30 30, 40 40, 50 50, 30 30))', 0));
insert into polygons_tbl values (st_linefromtext('linestring(10 10, 20 20, 30 40)', 4326));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(polygonz));
insert into polygons_tbl values (st_polygonfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
insert into polygons_tbl values (st_polygonfromtext('polygon((20 20 20, 30 30 30, 40 40 40, 20 20 20))', 4326));
insert into polygons_tbl values (st_polygonfromtext('polygon((30 30 30, 40 40 40, 50 50 50, 30 30 30))', 0));
insert into polygons_tbl values (st_linefromtext('linestring(10 10 10, 20 20 20, 30 30 30)'));
select * from polygons_tbl;
drop table polygons_tbl;

create table polygons_tbl(g geometry(polygonz, 4326));
insert into polygons_tbl values (st_polygonfromtext('polygon((10 10 10, 20 20 20, 30 30 30, 10 10 10))'));
insert into polygons_tbl values (st_polygonfromtext('polygon((20 20 20, 30 30 30, 40 40 40, 20 20 20))', 4326));
insert into polygons_tbl values (st_polygonfromtext('polygon((30 30 30, 40 40 40, 50 50 50, 30 30 30))', 0));
insert into polygons_tbl values (st_linefromtext('linestring(10 10 10, 20 20 20, 30 30 30)', 4326));
select * from polygons_tbl;
drop table polygons_tbl;

select st_polygonfromtext(geom) from geometriesTxt WHERE id=1;
select st_polygonfromtext(geom) from geometriesTxt WHERE id=2;
select st_polygonfromtext(geom) from geometriesTxt WHERE id=3;
select st_polygonfromtext(geom) from geometriesTxt WHERE id=4;
select st_polygonfromtext(geom) from geometriesTxt WHERE id=5;
select st_polygonfromtext(geom) from geometriesTxt WHERE id=6;
select st_polygonfromtext(geom) from geometriesTxt WHERE id=7;
select st_polygonfromtext(geom) from geometriesTxt WHERE id=8;
