@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\authorization.SF-1430616-data.sql"

@echo user=voc>		.monetdb
@echo password=voc>>	.monetdb

call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -q

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\authorization.SF-1430616-drop_user.sql"

@del .monetdb
