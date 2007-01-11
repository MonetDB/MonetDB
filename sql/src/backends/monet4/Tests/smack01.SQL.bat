@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

call Mlog.bat -x smack01.exe %MAPIPORT% sql
