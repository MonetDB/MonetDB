create procedure setmemorylimit(nbytes BIGINT) external name "io"."setmemorylimit";
call setmemorylimit(1000);
select name from tables where 1=0;
