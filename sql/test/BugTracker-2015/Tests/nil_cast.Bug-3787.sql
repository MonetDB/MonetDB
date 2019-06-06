START TRANSACTION;
create table foo (a smallint);
insert into foo values (1), (200);
select '',false,a=200 from foo union all select '',null,a=200 from foo;
select count(y),cast(sum(z)as bigint) from (select '',false,a=200 from foo union all select '',null,a=200 from foo) as t(x,y,z) group by x;
ROLLBACK;
