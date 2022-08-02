select 3836*8141;
--select 3836L*8141;

create table t( i int, l NUMERIC(20));
insert into t values(3836,8141);
insert into t values(1, 38368141);
insert into t values(1, 2147483647);
insert into t values(1, 2147483648);
insert into t values(1, 2147483649);
insert into t values(1, 21474000083649);
select * from t;
