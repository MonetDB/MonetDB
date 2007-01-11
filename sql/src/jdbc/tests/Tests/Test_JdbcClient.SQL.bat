@echo off

echo We don't know what the name of the JAR is here, and I don't know how
echo to figure that out using windows...  Sorry, this test fails on Windows.

set JAR=""
if exist "%SQL_PREFIX%\share\MonetDB\lib\jdbcclient-1.3.jar"	set JAR=%SQL_PREFIX%\share\MonetDB\lib\jdbcclient-1.3.jar
if exist "%MONETDB_PREFIX%\share\MonetDB\lib\jdbcclient-1.3.jar"	set JAR=%MONETDB_PREFIX%\share\MonetDB\lib\jdbcclient-1.3.jar
if JAR == ""							set JAR=%TSTTRGBASE%\share\MonetDB\lib\jdbcclient-1.3.jar

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\..\JdbcClient_create_tables.sql"

call Mlog.bat -x java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\..\JdbcClient_inserts_selects.sql"

call Mlog.bat -x java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -D

del .monetdb
