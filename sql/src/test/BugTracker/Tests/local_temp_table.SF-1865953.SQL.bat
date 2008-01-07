@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

call mclient -lsql -h %HOST% -p %MAPIPORT% -d %TSTDB% "%RELSRCDIR%\local_temp_table_data.SF-1865953.sql"
call mclient -lsql -h %HOST% -p %MAPIPORT% -d %TSTDB% "

@del .monetdb
