@echo off

set JAR="%CLIENTS_PREFIX%\share\MonetDB\lib\jdbcclient.jar"

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\cast_select.sql"

@del .monetdb
