start transaction;

create table foo (a int, b int);
select 1 + 1 as bar, cast(sum(b) as bigint) from foo group by bar;

rollback;
