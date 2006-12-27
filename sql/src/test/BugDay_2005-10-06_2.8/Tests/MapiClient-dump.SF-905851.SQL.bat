@echo off

call Mlog.bat -x %SQL_CLIENT% "%TSTSRCDIR%\JdbcClient_create_tables.sql"
call Mlog.bat -x %SQL_CLIENT% "%TSTSRCDIR%\JdbcClient_insert_selects.sql"
call Mlog.bat -x %SQL_DUMP%
