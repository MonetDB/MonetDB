
create table t (a integer);

select * from t where b = 0;
select * from t where t.b = 0;
select * from t where s.a = 0;

drop table t;
