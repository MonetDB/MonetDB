start transaction;
create table t1 (id int);
create table s1 (id varchar(23));
select * from t1 union select * from s1;

rollback;

start transaction;

create table f( id float);
create table b( id boolean);
select * from f union select * from b;

rollback;
