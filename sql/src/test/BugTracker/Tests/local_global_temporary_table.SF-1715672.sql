create LOCAL TEMPORARY table t1 (id int) ON COMMIT DROP;
create GLOBAL TEMPORARY table t1 (id int) ON COMMIT DROP;

drop table t1;
