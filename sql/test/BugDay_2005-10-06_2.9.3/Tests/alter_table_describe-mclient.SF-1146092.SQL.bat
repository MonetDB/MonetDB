@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

prompt # $t $g  
echo on

call %SQLCLIENT% -h%HOST% -p%MAPIPORT% -d%TSTDB% -i -e < "%TSTSRCDIR%\alter_table_describe.SF-1146092-src.sql"

@del .monetdb
