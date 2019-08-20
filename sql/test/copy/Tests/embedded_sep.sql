-- test the situation where separators are embedded in quoted fields
create table t_str(s string, t string);
copy 1 records into t_str from stdin delimiters ',',E'\n';
hello,world

copy 1 records into t_str from stdin delimiters ',',E'\n','"';
hello,world

copy 1 records into t_str from stdin delimiters ',',E'\n','"';
"hello","world"

copy 1 records into t_str from stdin delimiters ',',E'\n','"';
"hello,world","all"

copy 1 records into t_str from stdin delimiters ',',E'\n','"';
"hello\nworld","all"

select * from t_str;

copy 1 records into t_str from stdin delimiters ',',E'\n','"';
"hello\
world","all"

select * from t_str;

drop table t_str;
