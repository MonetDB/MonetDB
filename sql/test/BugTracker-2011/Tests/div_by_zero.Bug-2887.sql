Create table t2887 (c int);
Insert into t2887 values (0);
select min (case when "c" = 0 then 4 else 3/c end) from t2887;
drop table t2887;
