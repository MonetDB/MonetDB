@echo off

set PATH=%MONET_BUILD%\src\mapi\example\C;%MONET_PREFIX%\lib\MonetDB\Tests;%PATH%

call sample0.exe %HOST% %MAPIPORT% mil
