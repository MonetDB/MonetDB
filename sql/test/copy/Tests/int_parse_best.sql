create table t_int(i int);

-- If a numeric is enclosed as string we have to be careful
-- It seems that convention is to check for it, turning
-- the example below into one null value added to the table.
copy 1 records into t_int from stdin USING DELIMITERS ',','\n','\"' NULL AS '' best effort;
""

select * from t_int;
select * from sys.rejects;
call sys.clearrejects();
delete from t_int;

-- A decimal value entered at a place where we expect an int
-- should be recognized. It is the residu of dumping an int
-- as decimal.
copy 2 records into t_int from stdin USING DELIMITERS ',','\n','\"' NULL AS '' best effort;
3.00
-4.0000

select * from t_int;
select * from sys.rejects;
call sys.clearrejects();
delete from t_int;

copy 3 records into t_int from stdin USING DELIMITERS ',','\n','\"' NULL AS '' best effort;
0
5.1
9

select * from t_int;
select * from sys.rejects;
call sys.clearrejects();
delete from t_int;

copy 5 records into t_int from stdin delimiters ',','\n' NULL as '' best effort;
1
nil

null
abc

select * from t_int;
select * from sys.rejects;
call sys.clearrejects();
delete from t_int;

drop table t_int;
