start transaction;
create table t3addcolumn (name varchar(10));
insert into t3addcolumn values ('a'), ('b'), ('c');
select * from t3addcolumn;

create table t2addcolumn (name varchar(10), seq serial);
insert into t2addcolumn (name) select (name) from t3addcolumn;
select * from t2addcolumn;

create table t1addcolumn (name varchar(10));
insert into t1addcolumn (name) select (name) from t3addcolumn;
alter table t1addcolumn add column seq serial;
select * from t1addcolumn;
rollback;
