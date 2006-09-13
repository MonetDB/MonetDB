@echo off

call Mlog.bat -x %SQL_CLIENT% "%TSTSRCBASE%\src/jdbc/tests/JdbcClient_create_tables.sql"
call Mlog.bat -x %SQL_CLIENT% "%TSTSRCBASE%\src/jdbc/tests/JdbcClient_insert_selects.sql"
call Mlog.bat -x %SQL_DUMP%
