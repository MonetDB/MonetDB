@echo on
@prompt # $t $g  

	%SQL_CLIENT% -e -i < %TSTSRCDIR%\..\mclient-lsql-d.Bug-2861.sql
