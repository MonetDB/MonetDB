create table geo (g geometry(point));
insert into geo values(st_pointfromtext('point(10 20)'));
insert into geo values(st_point(20, 30));
insert into geo values(st_makepoint(30, 40));

select st_x(g) as X, st_y(g) as Y, st_Z(g) as Z, g from geo;

drop table geo;

create table geo (g geometry(pointz));
insert into geo values(st_pointfromtext('point(10 20 30)'));
insert into geo values(st_makepoint(30, 40, 50));

select st_x(g) as X, st_y(g) as Y, st_Z(g) as Z, g from geo;

drop table geo;


create table geo (g geometry(linestring));
insert into geo values (st_linefromtext('linestring(10 10, 20 20, 30 30)'));
select st_x(g) as X, st_y(g) as Y, st_Z(g) as Z, g from geo;
drop table geo;
