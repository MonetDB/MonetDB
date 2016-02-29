create table tbl1 (id integer, geom geometry(linestring));
create table tbl2 (id integer, geom geometry(linestring));

insert into tbl1 values(1,st_geomfromtext('LINESTRING (1 2, 1 5)'));
insert into tbl2 values(2,st_geomfromtext('LINESTRING (0 0, 4 3)'));
insert into tbl2 values(3,st_geomfromtext('LINESTRING (6 0, 6 5)'));
insert into tbl2 values(4,st_geomfromtext('LINESTRING (2 2, 5 6)'));

SELECT tbl1.id, tbl2.id, mbr(tbl1.geom) << mbr(tbl2.geom) AS overlaps FROM tbl1, tbl2;
SELECT tbl1.id, tbl2.id, tbl1.geom << tbl2.geom AS overlaps FROM tbl1, tbl2;

drop table tbl1;
drop table tbl2;
