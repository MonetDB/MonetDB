create table t3071 (s varchar(100));
insert into t3071 values ('42.42');
select avg ( cast (s as double)) from t3071;
drop table t3071;

