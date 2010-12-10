create table t2748 (x varchar(1));
copy 1 records into t2748 from stdin;
\\

select * from t2748;
drop table t2748;
