@echo off

set PATH=%CLIENTS_PREFIX%\lib\sql\Tests;%PATH%

call Mlog.bat -x testgetinfo.exe
