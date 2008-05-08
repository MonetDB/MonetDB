create table t1 (id int);
create table t2 (age int);
select age from t1, t2 where (age-id)/id<0.01;

drop table t1;
drop table t2;
