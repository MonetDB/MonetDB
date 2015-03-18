create table foo (a int);
insert into foo values (1),(2),(3);
select 100*v from (select sum(1) as v from foo group by a) as t;
drop table foo;
