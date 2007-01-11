@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

call Mlog.bat -x sample3.exe %HOST% %MAPIPORT% sql
