@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

call Mlog.bat -x sample2.exe %HOST% %MAPIPORT% mil
