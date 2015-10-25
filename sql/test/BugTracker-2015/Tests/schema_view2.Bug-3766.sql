create table t1 (i int);
create schema tst;
create view tst.v1 (i) as select * from t1;
select * from tst.v1;
select * from _tables where name like '%v1%';
create view tst.v1 (i) as select * from t1;
drop view tst.v1;

drop schema tst;
drop table t1;
