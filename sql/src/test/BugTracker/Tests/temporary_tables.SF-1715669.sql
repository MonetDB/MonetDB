create TEMPORARY table t1 (id int) ON COMMIT DROP;
select * from tables;

drop table t1;
