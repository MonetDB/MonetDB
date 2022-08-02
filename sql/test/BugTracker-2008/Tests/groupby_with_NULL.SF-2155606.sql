create table testola (color varchar(128), count integer);

insert into testola values ('blue', 12);
insert into testola values ('red', 2);

select color, null as something, cast( sum(count) as bigint) from testola group by color;

drop table testola;
