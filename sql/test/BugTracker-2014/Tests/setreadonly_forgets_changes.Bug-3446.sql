create table t3446(a int);
insert into t3446 values (1),(3),(2),(3),(3),(3);

select * from t3446;

-- remove the 3s
delete from t3446 where a = 3;
select * from t3446;

-- Oops, the 3s are back when the table is set read-only
alter table t3446 set read only;
select * from t3446;

-- Oops, the 3s are gone again when the table is read/write again
alter table t3446 set read write;
select * from t3446;

drop table t3446;
