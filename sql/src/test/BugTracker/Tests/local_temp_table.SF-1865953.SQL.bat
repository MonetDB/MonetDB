@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

call mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\local_temp_table_data.SF-1865953.sql"
call mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% "

@del .monetdb
