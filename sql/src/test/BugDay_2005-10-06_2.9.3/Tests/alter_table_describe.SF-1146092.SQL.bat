@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x mjclient -h %HOST% -p %MAPIPORT% -f "%RELSRCDIR%\alter_table_describe.SF-1146092-src.sql"

del .monetdb
