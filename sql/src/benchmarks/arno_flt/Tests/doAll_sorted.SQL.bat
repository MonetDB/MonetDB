@echo off

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCDIR%\check0.sql"

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\queries_sorted.sql"
