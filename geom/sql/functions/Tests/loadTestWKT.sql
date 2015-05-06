create table geometriesTxt(id serial, geom text);
insert into geometriesTxt(geom) values ('POINT(10 20)');
insert into geometriesTxt(geom) values ('LINESTRING(10 20, 30 40, 50 60)');
insert into geometriesTxt(geom) values ('POLYGON((10 10, 10 20, 20 20, 20 10, 10 10))');
insert into geometriesTxt(geom) values ('MULTIPOINT(10 20, 30 40)');
insert into geometriesTxt(geom) values ('MULTILINESTRING((30 40, 40 50, 30 40), (50 60, 40 50, 20 30, 50 60))');
insert into geometriesTxt(geom) values ('MULTIPOLYGON(((10 10, 10 20, 20 20, 20 10, 10 10),(30 30, 30 40, 40 40, 40 30, 30 30)))');
insert into geometriesTxt(geom) values ('MULTIPOLYGON EMPTY');
insert into geometriesTxt(geom) values ('GEOMETRYCOLLECTION(POINT(10 20),LINESTRING(10 20, 30 40),POLYGON((10 10, 10 20, 20 20, 20 10, 10 10)))');
