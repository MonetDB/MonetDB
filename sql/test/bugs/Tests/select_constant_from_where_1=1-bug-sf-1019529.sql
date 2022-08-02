create table t1019529 (name varchar(1024));
insert into t1019529 values ('niels'), ('fabian'), ('martin');

select name from t1019529 where name='doesnotexist';
select 1 from t1019529 where name='doesnotexist';
select 1 from t1019529;
select 1 from t1019529 where 0=1;
select 1 from t1019529 where 1=1;
select 1 where 0=1;
select 1 where 1=1;
select 1, count(*) from t1019529;
select name, count(*) from t1019529; -- should fail
select 1, count(*) from t1019529 where 0=1;
select 1, count(*) where 0=1;

drop table t1019529;
