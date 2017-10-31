@echo off

set URL=jdbc:monetdb://%HOST%:%MAPIPORT%/%TSTDB%?user=monetdb^&password=monetdb%JDBC_EXTRA_ARGS%

prompt # $t $g  
echo on

java %TST% "%URL%"
