@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

set PYTHONPATH=%CLIENTS_PREFIX%\%PYTHON_LIBDIR%

set v=4
if     "%TST_FIVE%" == "Five" set v=5
call Mlog.bat -x sqlsample.py %GDK_DBFARM% %TSTDB% %v%
