start transaction;
create table t1 (name varchar(10));
alter table t1 add column seq serial;

create table t2 (name varchar(10), seq serial);
create table t3 (name varchar(10));
insert into t3 values ('a'), ('b'), ('c');
insert into t2 (name) select (name) from t3;
rollback;
