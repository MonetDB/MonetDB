@echo off

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\create_tables.flt.sql"

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCDIR%\check0.sql"

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\insert_MODEL.flt.sql"

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\insert_ATOM.flt.sql"

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\insert_BOND.flt.sql"

call Mlog.bat -x %SQL_CLIENT% < "%TSTSRCDIR%\check0.sql"
