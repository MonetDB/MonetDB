@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

prompt # $t $g  
echo on

sample4.exe %HOST% %MAPIPORT% mil
