@prompt # $t $g
@echo on

%SQL_CLIENT% -e < "%TSTSRCDIR%\..\monetdb\%TST%.sql"
