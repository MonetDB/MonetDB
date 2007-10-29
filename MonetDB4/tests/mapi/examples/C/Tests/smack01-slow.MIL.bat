@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

prompt # $t $g  
echo on

smack01.exe %MAPIPORT%
