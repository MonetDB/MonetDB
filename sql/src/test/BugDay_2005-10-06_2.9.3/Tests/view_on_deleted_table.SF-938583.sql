create table t (i int);
create view t2 as select * from t;
drop table t;
select * from t2;
