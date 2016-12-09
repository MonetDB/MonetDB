-- rediculous limit setting, should fail to start server
call setmemorylimit(0);
select name from tables where 1=0;
