call Mlog.bat "echo 'loaded(); quit();' \| Mserver %setMONETDB_MOD_PATH% --set monet_prompt='' --set sql_logdir=/xyz --dbinit='module(sql_server); loaded();'"
call           echo 'loaded(); quit();'  | Mserver %setMONETDB_MOD_PATH% --set monet_prompt='' --set sql_logdir=/xyz --dbinit='module(sql_server); loaded();'
               
