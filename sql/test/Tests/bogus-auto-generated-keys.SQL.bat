@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

prompt # $t $g  
echo on

call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%TSTSRCBASE%\%TSTDIR%\bogus-auto-generated-keys.sql"

@del .monetdb
