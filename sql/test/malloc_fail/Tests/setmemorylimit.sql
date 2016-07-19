create procedure setmemorylimit(nbytes BIGINT) external name "io"."setmemorylimit";
call setmemorylimit(100000000);
select name from tables where 1=0;
call setmemorylimit(-1);
