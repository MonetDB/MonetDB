create table t1096337(i int);
insert into t1096337 values(123456789);
select * from t1096337;
delete from t1096337;
select * from t1096337;

start transaction;
create table u1096337 (id int);
commit;              
insert into u1096337 values (123456789);
select * from u1096337;
delete from u1096337;
select * from u1096337;
