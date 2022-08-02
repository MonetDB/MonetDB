CREATE TABLE t2795 (x INTEGER, y INTEGER);
alter table t2795 add r float default sqrt(power(t2795.x, 2) + power(t2795.y, 2));

drop table t2795;
