drop table if exists t;

start transaction;
create table t ( c1 int , c2 int );
copy 1 records into t (c1) from stdin (c1,c2) best effort;
1|2
select * from t;
rollback;

start transaction;
create table t ( c1 int , c2 int );
copy 1 records into t (c2) from stdin (c1,c2) best effort;
1|2
select * from t;
rollback;

start transaction;
create table t ( c1 int , c2 int );
copy 1 records into t (c1,c2) from stdin (x,c1,c2) best effort;
0|1|2
select * from t;
rollback;

start transaction;
create table t ( c1 int , c2 int );
copy 1 records into t (c1,c2) from stdin (c1,x,c2) best effort;
1|0|2
select * from t;
rollback;

start transaction;
create table t ( c1 int , c2 int );
copy 1 records into t (c1,c2) from stdin (c1,c2,x) best effort;
1|2|0
select * from t;
rollback;
