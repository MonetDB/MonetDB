@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

call Mlog.bat -x mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\authorization.SF-1430616-data.sql"

echo user=voc>		.monetdb
echo password=voc>>	.monetdb

call Mlog.bat -x mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -q

del .monetdb
