@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

set PYTHONPATH=%MONETDB_PREFIX%\%PYTHON_LIBDIR%;%CLIENTS_PREFIX%\%PYTHON_LIBDIR%;%CLIENTS_PREFIX%\%PYTHON_LIBDIR%\MonetDB;%SQL_PREFIX%\%PYTHON_LIBDIR%;%PYTHONPATH%

set v=4
if     "%TST_FIVE%" == "Five" set v=5

prompt # $t $g  
echo on

sqlsample.py "%GDK_DBFARM%" %TSTDB% %v%
