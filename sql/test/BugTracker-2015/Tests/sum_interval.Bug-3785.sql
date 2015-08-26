create table foo (a timestamp,b timestamp);
insert into foo values ('2000-1-1','2001-1-1');
select sum(b-a) from foo;
drop table foo;
select sum(i) from (select interval '1' day) x(i);
