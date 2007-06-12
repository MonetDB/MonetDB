create table t1 (id int, name varchar(1024), PRIMARY KEY(id));
 
create table t2 (id int DEFAULT 13, age int, PRIMARY KEY (ID), FOREIGN 
KEY(id) REFERENCES t1(id) ON UPDATE SET DEFAULT ON DELETE SET DEFAULT);
 
insert into t1 values(1, 'monetdb');
 
insert into t2 values(1, 23);
 
update t2 set id = 2 where id =1;
 
select * from t1;
select * from t2;

drop table t2;
drop table t1;
