create table t1 (id int NOT NULL);
insert into t1 values ((select id, name from tables));
drop table t1;
