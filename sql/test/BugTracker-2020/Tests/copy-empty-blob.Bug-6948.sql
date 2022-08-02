start transaction;
create table blobtbl (i int, b blob);

-- the following COPY INTO should succeed
copy into blobtbl from stdin delimiters ',','\n','"';
0,NULL
1,12ff
2,""
3,

-- This should return 4 rows
select * form blobtbl;
rollback;
