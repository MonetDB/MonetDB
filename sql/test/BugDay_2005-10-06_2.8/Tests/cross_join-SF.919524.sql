create table t(i int, s varchar(10));
create table s(j int);
select * from t cross join s;
drop table t;
drop table s;
