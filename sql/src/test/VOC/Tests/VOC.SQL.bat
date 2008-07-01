@echo off

echo user=monetdb>	.monetdb
echo password=monetdb>>	.monetdb

set LANG=en_US.UTF-8

prompt # $t $g  
echo on

call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\VOCcreate_user.sql"

@echo user=voc>		.monetdb
@echo password=voc>>	.monetdb

call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\VOCschema.sql"
call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\VOCinsert.sql"
call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\VOCquery.sql"
call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\VOCmanual_examples.sql"
call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\VOCdrop.sql"

@echo user=monetdb>	.monetdb
@echo password=monetdb>> .monetdb

call java nl.cwi.monetdb.client.JdbcClient -h %HOST% -p %MAPIPORT% -d %TSTDB% -e -f "%RELSRCDIR%\..\VOCdrop_user.sql"

@del .monetdb
