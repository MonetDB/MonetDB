start transaction;
create table t3 (name varchar(10));
insert into t3 values ('a'), ('b'), ('c');
select * from t3;

create table t2 (name varchar(10), seq serial);
insert into t2 (name) select (name) from t3;
select * from t2;

create table t1 (name varchar(10));
insert into t1 (name) select (name) from t3;
alter table t1 add column seq serial;
select * from t1;
rollback;
