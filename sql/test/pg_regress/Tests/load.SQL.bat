@prompt # $t $g
@echo on

%SQL_CLIENT% < "%TSTSRCDIR%/create_table.sql"

%SQL_CLIENT% < "%TSTTRGDIR%/load.copy.source"

%SQL_CLIENT% -s "select count(*) from aggtest;"
