-- test the situation where the number of column values are incorrect
create table t_columns(i int, t string);

copy 7 records into t_columns from stdin delimiters ',','\n' best effort;
1,hello
2
no tag
3,too much,xyz
4,world
5,wereld
6,maan

select * from t_columns;

select * from sys.rejects();

drop table t_columns;
