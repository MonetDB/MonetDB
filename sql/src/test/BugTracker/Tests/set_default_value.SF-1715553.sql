create table t1(id int, name varchar(1024), age int );

alter table t1 alter id set DEFAULT RANK() OVER (PARTITION BY id, age ORDER
BY id DESC);

drop table t1;
