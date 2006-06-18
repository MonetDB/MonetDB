start transaction;

create table t1314649 (name varchar(1024), human boolean);
insert into t1314649 values ('niels', true), ('fabian', false), ('martin', NULL);

set explain='profile';
select 1;
select query, user from profile;
select count(*) from t1314649 where "human" = true;
select query, user from profile;
