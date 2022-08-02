start transaction;

create table t1314649 (name varchar(1024), human boolean);
insert into t1314649 values ('niels', true), ('fabian', false), ('martin', NULL);

record select 1;
select query, user from history;
record select count(*) from t1314649 where "human" = true;
select query, user from history;
