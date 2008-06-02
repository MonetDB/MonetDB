@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

call mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\authorization.SF-1430616-data.sql"

@echo user=voc>		.monetdb
@echo password=voc>>	.monetdb

call mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -q

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

call mjclient -h %HOST% -p %MAPIPORT% -d %TSTDB% -f "%RELSRCDIR%\authorization.SF-1430616-drop_user.sql"

@del .monetdb
