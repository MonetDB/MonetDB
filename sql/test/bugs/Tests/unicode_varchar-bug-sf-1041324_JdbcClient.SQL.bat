@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

prompt # $t $g  
echo on

call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\..\unicode_varchar-bug-sf-1041324.sql"

@del .monetdb
