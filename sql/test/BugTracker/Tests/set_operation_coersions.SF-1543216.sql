start transaction;
create table t1543216 (id int);
create table s1543216 (id varchar(23));
select * from t1543216 union select * from s1543216;

rollback;

start transaction;

create table f1543216( id float);
create table b1543216( id boolean);
select * from f1543216 union select * from b1543216;

rollback;
