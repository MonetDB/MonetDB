create table t11959357 (id int);
create table t21959357 (age int);
select age from t11959357, t21959357 where (age-id)/id<0.01;

drop table t11959357;
drop table t21959357;
