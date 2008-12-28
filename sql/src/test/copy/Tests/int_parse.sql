create table t_int(i int);
copy 3 records into t_int from stdin delimiters ',','\n' NULL as '';
1
nil
null
abc

select * from t_int;

drop table t_int;
