create table tbls (id int);
insert into tbls values (0),(1),(2);
create table t1 (id int NOT NULL);
insert into t1 values ((select id from tbls));
drop table t1;
drop table tbls;
