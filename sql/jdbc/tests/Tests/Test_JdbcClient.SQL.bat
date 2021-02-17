@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

prompt # $t $g  
echo on

java org.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" --help

java org.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%TSTSRCBASE%\%TSTDIR%\Tests\JdbcClient_create_tables.sql"
java org.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%TSTSRCBASE%\%TSTDIR%\Tests\JdbcClient_inserts_selects.sql"
java org.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -D
java org.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%TSTSRCBASE%\%TSTDIR%\Tests\JdbcClient_drop_tables.sql"

@del .monetdb
