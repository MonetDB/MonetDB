-- test the situation where escapes are embedded in (quoted) fields
start transaction;
create table tt_str(s string, t string);
copy 3 records into tt_str from stdin delimiters ',',E'\n';
\\hello,world
hello\t,world
hello\n,world

copy 1 records into tt_str from stdin delimiters ',',E'\n','"';
"hello\"","world"

copy 1 records into tt_str from stdin delimiters ',',E'\n','"';
hello\,world,"all"

copy 1 records into tt_str from stdin delimiters ',',E'\n','"';
hello\,world,"all\
therest"

select * from tt_str;

drop table tt_str;
rollback;
