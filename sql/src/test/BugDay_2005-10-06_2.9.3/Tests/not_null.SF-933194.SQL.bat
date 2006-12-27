@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x mjclient -h %HOST% -p %MAPIPORT% -f "%RELSRCDIR%\not_null.SF-933194-src.sql"

del .monetdb
