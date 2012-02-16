@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

prompt # $t $g  
echo on

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%TSTSRCBASE%\%TSTDIR%\JdbcClient_create_tables.sql"

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%TSTSRCBASE%\%TSTDIR%\JdbcClient_inserts_selects.sql"

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -D

@del .monetdb
