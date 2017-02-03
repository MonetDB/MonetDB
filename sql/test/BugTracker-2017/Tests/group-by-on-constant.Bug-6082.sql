start transaction;

create table foo (a int, b int);
select 1 + 1 as bar, sum(b) from foo group by bar;

rollback;
