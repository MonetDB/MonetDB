@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%RELSRCDIR%\..\JdbcClient_create_tables.sql"

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%RELSRCDIR%\..\JdbcClient_inserts_selects.sql"

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -D

@del .monetdb
