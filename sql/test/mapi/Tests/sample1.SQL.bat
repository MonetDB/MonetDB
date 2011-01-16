@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

prompt # $t $g  
echo on

sample1.exe %HOST% %MAPIPORT% sql
