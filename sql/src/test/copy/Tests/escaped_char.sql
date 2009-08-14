-- test the situation where escapes are embedded in (quoted) fields
create table t_str(s string, t string);
copy 3 records into t_str from stdin delimiters ',','\n';
\\hello,world
hello\t,world
hello\n,world

copy 1 records into t_str from stdin delimiters ',','\n','"';
"hello\"","world"

copy 1 records into t_str from stdin delimiters ',','\n','"';
hello\,world,"all"

copy 1 records into t_str from stdin delimiters ',','\n','"';
hello\,world,"all\
therest"

select * from t_str;

drop table t_str;
