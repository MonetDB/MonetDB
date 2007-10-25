@echo off

set URL=jdbc:monetdb://%HOST%:%MAPIPORT%/%TSTDB%?user=monetdb^&password=monetdb%JDBC_EXTRA_ARGS%

call Mlog.bat -x java -classpath "%CLIENTS_PREFIX%\share\MonetDB\lib\monetdb-1.6-jdbc.jar;%CLIENTS_PREFIX%\share\MonetDB\Tests" %TST% "%URL%"
