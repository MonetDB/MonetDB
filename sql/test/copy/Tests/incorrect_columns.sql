-- test the situation where the number of column values are incorrect
create table t_columns(i int, t string);

copy 5 records into t_columns from stdin delimiters ',','\n' best effort;
1,hello
2
no tag
3,too much, data
4,world

select * from t_columns;

select * from sys.rejects();

drop table t_columns;
