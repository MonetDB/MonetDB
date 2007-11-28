@echo off

set URL=jdbc:monetdb://%HOST%:%MAPIPORT%/%TSTDB%?user=monetdb^&password=monetdb%JDBC_EXTRA_ARGS%

prompt # $t $g  
echo on

java -classpath "%MONETDB_JAVA_PREFIX%\share\MonetDB\lib\monetdb-1.7-jdbc.jar;%MONETDB_JAVA_PREFIX%\share\MonetDB\Tests" %TST% "%URL%"
