create table t3850 (
    s varchar(1)
);
insert into t3850 values (code(127));

\D

drop table t3850;
