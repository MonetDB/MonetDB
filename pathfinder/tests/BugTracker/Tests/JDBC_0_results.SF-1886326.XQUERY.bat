@echo off

echo user=monetdb>      .monetdb
echo password=monetdb>> .monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -l xquery -f "%TST%.0.xq"

@del .monetdb
