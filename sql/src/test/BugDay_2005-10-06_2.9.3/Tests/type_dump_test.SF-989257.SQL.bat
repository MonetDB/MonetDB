@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\type_dump_test.SF-989257-src.sql"

del .monetdb
