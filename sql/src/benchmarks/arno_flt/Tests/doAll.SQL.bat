@echo off

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCDIR%\check0.sql"

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCDIR%\queries.sql"
