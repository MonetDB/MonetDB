@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\jdbc_no_debug.SF-1739356-data.sql"

del .monetdb
