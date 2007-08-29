@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

call Mlog.bat -x sample4.exe %HOST% %MAPIPORT% mil
