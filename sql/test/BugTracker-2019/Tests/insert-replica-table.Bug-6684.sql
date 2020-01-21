create replica table t1 (a int);
insert into t1 values (1);
update t1 set a = 2;
delete from t1;
insert into t1 values (1);
truncate t1;
drop table t1;
