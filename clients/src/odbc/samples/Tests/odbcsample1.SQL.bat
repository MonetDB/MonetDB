@echo off

set PATH=%TSTBLDBASE%\src\odbc\samples;%SQL_PREFIX%\lib\sql\Tests;%PATH%

call Mlog.bat -x odbcsample1.exe
