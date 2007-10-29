@echo off

set JAR="%CLIENTS_PREFIX%\share\MonetDB\lib\jdbcclient.jar"

echo user=monetdb>      .monetdb
echo password=monetdb>> .monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -l xquery -f "%TST%.250.xq"

java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -l xquery -f "%TST%.251.xq"

@del .monetdb
