START TRANSACTION;
create table foo (a smallint);
select count(y),sum(z) from (select '',false,a=200 from foo union all select '',null,a=200 from foo) as t(x,y,z) group by x;
ROLLBACK;
