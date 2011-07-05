create table geom_bug2814(id int, g Geometry);
insert into geom_bug2814 values(1, GeomFromText('POINT(4 5)', 0));
insert into geom_bug2814 values(2, GeomFromText('POLYGON((0 0, 10 0, 0 10, 0 0))', 0));
insert into geom_bug2814 values(3, null);

select id, mbr(g) from geom_bug2814 where id=1 or id=2;
select id, mbr(g) from geom_bug2814 where id=3;

drop table geom_bug2814;
