start transaction;
create table tmpcopy(i integer, d decimal(8,3));
copy 10 records into tmpcopy from stdin delimiters ',','\n';
0,1.2
0,2.34
 0,3.456
0, 4.456
0,5
0 ,67
0,890
0,5 
0,67 
0,890
select * from tmpcopy;
-- and too many digits (should fail)
copy 1 records into tmpcopy from stdin delimiters ',','\n';
0,12.3456
rollback;
