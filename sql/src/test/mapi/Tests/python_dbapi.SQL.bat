@echo off

set PATH=%PREFIX%\lib\MonetDB\Tests;%PATH%

set PYTHONPATH=%PREFIX%\%PYTHON_LIBDIR%;%PREFIX%\%PYTHON_LIBDIR%\MonetDB;%PYTHONPATH%

prompt # $t $g  
echo on

sqlsample.py %MAPIPORT% %TSTDB%
