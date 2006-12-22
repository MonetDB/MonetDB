@echo off

if exist "%MONETDB_BUILD%\src\mapi\clients\java\mapi.jar" goto l1
set CLASSPATH=%MONETDB_PREFIX\share\MonetDB\lib\mapi.jar;%CLASSPATH%
goto s1
:l1
set CLASSPATH=%MONETDB_BUILD%\src\mapi\clients\java\mapi.jar;%CLASSPATH%
:s1

set LANG=en_US.UTF-8

echo print("\303\251\303\251n"); | java MapiClient localhost %MAPIPORT% guest anonymous mil --utf8
