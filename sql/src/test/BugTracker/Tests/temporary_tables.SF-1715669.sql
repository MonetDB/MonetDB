create TEMPORARY table t1 (id int) ON COMMIT DROP;
select name from tables where name = 't1';

drop table t1;
