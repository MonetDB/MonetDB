call Mlog.bat "echo 'loaded(); quit();' \| Mserver --set mapi_port=%MAPIPORT% %setGDK_DBFARM% %setMONETDB_MOD_PATH% --set monet_prompt='' --set sql_logdir=/xyz --dbinit='module(sql_server); loaded();'"
call           echo 'loaded(); quit();'  | Mserver --set mapi_port=%MAPIPORT% %setGDK_DBFARM% %setMONETDB_MOD_PATH% --set monet_prompt='' --set sql_logdir=/xyz --dbinit='module(sql_server); loaded();'
               
