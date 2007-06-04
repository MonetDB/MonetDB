@echo off

set JAR="%CLIENTS_PREFIX%\share\MonetDB\lib\jdbcclient-1.5.jar"

echo user=monetdb>      .monetdb
echo password=monetdb>> .monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -l xquery -f "%TST%.250.xq"

call Mlog.bat -x java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -d %TSTDB% -l xquery -f "%TST%.251.xq"

del .monetdb
