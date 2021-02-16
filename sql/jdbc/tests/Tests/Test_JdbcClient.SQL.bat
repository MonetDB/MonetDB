@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

prompt # $t $g  

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" --help

java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%TSTSRCBASE%\%TSTDIR%\Tests\JdbcClient_create_tables.sql"
java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%TSTSRCBASE%\%TSTDIR%\Tests\JdbcClient_inserts_selects.sql"
java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -D
java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%TSTSRCBASE%\%TSTDIR%\Tests\JdbcClient_drop_tables.sql"

@del .monetdb
