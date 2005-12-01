@echo off

set PATH=%MONETDB_BUILD%\src\mapi\examples\C;%MONETDB_PREFIX%\lib\MonetDB\Tests;%PATH%

call Mlog.bat -x sample0.exe %HOST% %MAPIPORT% mil
