select st_makeline(st_geomfromtext('point(10 20)'), st_geomfromtext('point(30 40)'));
select st_makeline(st_geomfromtext('point(10 20)'), st_geomfromtext('point(30 40)', 28992));

select st_makeline(st_geomfromtext('linestring(10 20, 30 40, 50 60)'), st_geomfromtext('linestring(100 200, 300 400, 500 600)'));

create table points(p Geometry(POINT));
insert into points values(st_geomfromtext('point(10 20)'));
insert into points values(st_geomfromtext('point(20 30)'));
insert into points values(st_geomfromtext('point(30 40)'));
insert into points values(st_geomfromtext('point(40 50)'));

create table points2(p Geometry(POINT));
insert into points2 values(st_geomfromtext('point(100 200)'));
insert into points2 values(st_geomfromtext('point(200 300)'));
insert into points2 values(st_geomfromtext('point(300 400)'));
insert into points2 values(st_geomfromtext('point(400 500)'));

select st_makeline(p) from points;
select st_makeline(points.p, points2.p) from points, points2 where st_X(points2.p) = 10*st_X(points.p);


drop table points;
drop table points2;
