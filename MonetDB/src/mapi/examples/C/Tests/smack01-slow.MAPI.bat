@echo off

set PATH=%MONET_BUILD%\src\mapi\examples\C;%MONET_PREFIX%\lib\MonetDB\Tests;%PATH%

call Mlog.bat -x smack01.exe %MAPIPORT%
