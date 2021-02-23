@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

prompt # $t $g  

call java org.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%TSTSRCBASE%\%TSTDIR%\Tests\bogus-auto-generated-keys.sql"

@del .monetdb
