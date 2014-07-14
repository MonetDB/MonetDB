create table t1 (id int NOT NULL);
insert into t1 values ((select id from tables));
drop table t1;
