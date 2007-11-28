@echo off

set JAR=%MONETDB_JAVA_PREFIX%\share\MonetDB\lib\jdbcclient.jar

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%RELSRCDIR%\..\JdbcClient_create_tables.sql"

java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -f "%RELSRCDIR%\..\JdbcClient_inserts_selects.sql"

java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d "%TSTDB%" -D

@del .monetdb
