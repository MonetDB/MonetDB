START TRANSACTION;
select null;
select null as v;
create table t (i integer);
insert into t values (null);
select * from t where i is null;
