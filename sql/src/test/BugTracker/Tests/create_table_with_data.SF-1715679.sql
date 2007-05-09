create table t1 as select id from tables order by id asc with data;
select * from t1;

drop table t1;
