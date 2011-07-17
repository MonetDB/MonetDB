create table bug2058a (s varchar(10));
create table bug2058b (s varchar(10));
insert into bug2058a values ('bad');
insert into bug2058b values ('good');
-- this should result in the contents of table bug2058b, not bug2058a
with bug2058a as (select * from bug2058b) select * from bug2058a;
-- clean up
drop table bug2058a;
drop table bug2058b;
