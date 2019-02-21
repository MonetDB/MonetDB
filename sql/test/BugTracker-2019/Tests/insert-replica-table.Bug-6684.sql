start transaction;
create replica table t1 (a int);
insert into t1 values (1);
rollback;
