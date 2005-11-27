
create table t(i int);
insert into t values(0);
insert into t values(1);
insert into t values(2);
insert into t values(3);
insert into t values(4);

select case i
when 0 then 'base table'
when 1 then 'system table'
when 2 then 'view'
when 3 then 'session temporary table'
when 4 then 'temporary table'
from t;

select case i
when 0 then 'base table'
when 1 then 'system table'
when 2 then 'view'
when 3 then 'session temporary table'
when 4 then 'temporary table'
end
from t;

drop table t;
