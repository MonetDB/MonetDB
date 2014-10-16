start transaction;
create table t(r integer,a integer);
insert into t values (1,42),(2,0),(3,null); 
select * from t;
-- this works
select case when not (a is null) and a > 0.0 then r/a else -1 end as s from t;
-- this does not
select case when not (a is null) and a > 0.0 then 1.0*r/a else -1 end as r from t;
rollback;
