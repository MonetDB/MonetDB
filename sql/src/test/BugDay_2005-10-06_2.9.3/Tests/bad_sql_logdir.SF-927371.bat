call Mlog.bat "echo 'loaded(); quit();' \| Mserver --set monet_prompt='' --set sql_logdir=/xyz --dbinit='module(sql_server); loaded(); sql_server_start();'"
call           echo 'loaded(); quit();' \| Mserver --set monet_prompt='' --set sql_logdir=/xyz --dbinit='module(sql_server); loaded(); sql_server_start();'
               