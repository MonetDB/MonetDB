@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\except-union-intersect-bug-sf-1146079.sql"

del .monetdb
