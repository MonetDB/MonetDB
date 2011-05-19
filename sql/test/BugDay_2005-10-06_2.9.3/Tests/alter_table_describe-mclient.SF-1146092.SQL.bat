@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

call %SQLCLIENT% -h%HOST% -p%MAPIPORT% -d%TSTDB% -ftest -i -e < "%TSTSRCDIR%\alter_table_describe.SF-1146092-src.sql"

@del .monetdb
