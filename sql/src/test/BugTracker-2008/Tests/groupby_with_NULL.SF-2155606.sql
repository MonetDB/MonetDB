create table testola (color varchar(128), count integer);

insert into testola values ('blue', 12);
insert into testola values ('red', 2);

select color, null, sum(count) from testola group by color;

drop table testola;
