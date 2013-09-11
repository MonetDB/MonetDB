start transaction;

create table t3335(x integer check(x > 0 and x < 2));
insert into t3335 values(1);
insert into t3335 values(0);
insert into t3335 values(2);
insert into t3335 values(-1);
insert into t3335 values(3);

rollback;
