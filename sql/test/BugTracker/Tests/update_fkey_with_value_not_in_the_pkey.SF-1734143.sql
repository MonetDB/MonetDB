create table t1734143a (id int, name varchar(1024), PRIMARY KEY(id));
 
create table t1734143b (id int DEFAULT 13, age int, PRIMARY KEY (ID), FOREIGN 
KEY(id) REFERENCES t1734143a(id) ON UPDATE SET DEFAULT ON DELETE SET DEFAULT);
 
insert into t1734143a values(1, 'monetdb');
 
insert into t1734143b values(1, 23);
 
update t1734143b set id = 2 where id =1;
 
select * from t1734143a;
select * from t1734143b;

drop table t1734143b;
drop table t1734143a;
