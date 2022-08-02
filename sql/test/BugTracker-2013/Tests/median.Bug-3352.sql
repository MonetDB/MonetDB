start transaction;

create table t3352 (a numeric(10,2),b double);

insert into t3352 values (41.18,41.18);
insert into t3352 values (31.13,31.13);
insert into t3352 values (21.22,21.22);
insert into t3352 values (31.4,31.4);
insert into t3352 values (121.5,121.5);
insert into t3352 values (111.6,111.6);
insert into t3352 values (222.8,222.8);

select median(a) from t3352;
select median(b) from t3352;

rollback;
