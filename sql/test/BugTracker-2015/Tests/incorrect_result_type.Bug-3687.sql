create table foo (a int);
insert into foo values (1),(2),(3);
select cast(sum(1) as bigint) as v from foo group by a;
select cast(100*v as bigint) from (select sum(1) as v from foo group by a) as t;
drop table foo;
