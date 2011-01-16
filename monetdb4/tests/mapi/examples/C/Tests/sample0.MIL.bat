@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

prompt # $t $g  
echo on

sample0.exe %HOST% %MAPIPORT% mil
