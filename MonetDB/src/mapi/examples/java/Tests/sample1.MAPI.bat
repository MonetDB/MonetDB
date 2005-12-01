@echo off

if exist "%MONETDB_BUILD%\src\mapi\clients\java\mapi.jar" goto l1
set CLASSPATH=%MONETDB_PREFIX\share\MonetDB\lib\mapi.jar;%CLASSPATH%
goto s1
:l1
set CLASSPATH=%MONETDB_BUILD%\src\mapi\clients\java\mapi.jar;%CLASSPATH%
:s1

if exist "%MONETDB_BUILD%\src\mapi\examples\java\sample.class" goto l2
set CLASSPATH=%MONETDB_PREFIX\share\MonetDB\Tests;%CLASSPATH%
goto s2
:l2
set CLASSPATH=%MONETDB_BUILD%\src\mapi\examples\java;%CLASSPATH%
:s2

call Mlog.bat -x java sample %HOST% %MAPIPORT% true
