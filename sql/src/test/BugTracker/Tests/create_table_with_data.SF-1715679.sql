create table t1 as select name from tables order by name asc with data;
--create table t1 as select id from tables with data;
select name from t1;

drop table t1;
