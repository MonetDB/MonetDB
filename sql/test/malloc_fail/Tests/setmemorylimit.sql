create procedure setmemorylimit(nbytes BIGINT) external name "io"."setmemorylimit";
call setmemorylimit(0);
call setmemorylimit(100000000);
-- this should work fine
select name from tables where 1=0;
call setmemorylimit(0);
drop procedure setmemorylimit;
