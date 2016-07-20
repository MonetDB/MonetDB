create procedure setmemorylimit(nbytes BIGINT) external name "io"."setmemorylimit";
call setmemorylimit(0);
select name from tables where 1=0;
