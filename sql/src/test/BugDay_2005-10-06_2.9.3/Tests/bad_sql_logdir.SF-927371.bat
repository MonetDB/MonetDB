@prompt # $t $g  
@echo on

call echo sql.start(); clients.quit(); | mserver5 --set mapi_port=%MAPIPORT% %setGDK_DBFARM% %setMONETDB_MOD_PATH% --set monet_prompt= --set sql_logdir=/xyz "--dbinit=include sql;"
               
