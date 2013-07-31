create table x (a integer);
create table y (b integer);
insert into x values (1), (2), (3), (4);
insert into y values (1), (2), (3), (4);
select * from x;
select * from y;
select a from y;
delete from x where a in (select nonexistant from y);
delete from x where a in (select a from y);
select * from x;
insert into x values (1), (2), (3), (4);
delete from x where a in (select a from y where a < 10);
select * from x;

drop table x;
drop table y;
