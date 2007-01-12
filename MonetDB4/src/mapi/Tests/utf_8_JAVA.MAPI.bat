@echo off

set JAR="%CLIENTS_PREFIX%\share\MonetDB\lib\jdbcclient-1.5.jar"

set LANG=en_US.UTF-8

echo user=guest>      .monetdb
echo password=anonymous>> .monetdb

echo print("\303\251\303\251n");> %TST%.mil

call java -jar "%JAR%" -h %HOST% -p %MAPIPORT% -l mil -f %TST%.mil
