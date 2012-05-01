create table t (s varchar(100));
insert into t values ('42.42');
select avg ( cast (s as double)) from t;
drop table t;

