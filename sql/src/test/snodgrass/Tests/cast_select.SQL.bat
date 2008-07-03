@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\cast_select.sql"

@del .monetdb
