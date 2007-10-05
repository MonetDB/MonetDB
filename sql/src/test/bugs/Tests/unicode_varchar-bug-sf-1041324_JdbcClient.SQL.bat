@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\..\unicode_varchar-bug-sf-1041324.sql"

del .monetdb
