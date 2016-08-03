-- error as there is no data for the s column (which we later skip)
create table tbl (i int, s string, d decimal(5, 2));
copy 3 records into tbl (i, d) from stdin delimiters ',','\n';
1,2.0
2,2.1
3,2.2
select * from tbl;
drop table tbl;

-- s column has data but we do not use it
create table tbl (i int, s string, d decimal(5, 2));
copy 3 records into tbl (i, d) from stdin delimiters ',','\n';
1,"test",2.0
2,"test1",2.1
3,"test",2.2
select * from tbl;
drop table tbl;

-- s not given and not used
create table tbl (i int, s string, d decimal(5, 2));
copy 3 records into tbl (i, d) from stdin (i, d) delimiters ',','\n';
1,2.0
2,2.1
3,2.2
select * from tbl;
drop table tbl;
