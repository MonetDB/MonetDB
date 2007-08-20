@echo off

set JAR="%CLIENTS_PREFIX%\share\MonetDB\lib\jdbcclient-1.6.jar"

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\..\JdbcClient_create_tables.sql"

call Mlog.bat -x java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\..\JdbcClient_inserts_selects.sql"

call Mlog.bat -x java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -D

del .monetdb
