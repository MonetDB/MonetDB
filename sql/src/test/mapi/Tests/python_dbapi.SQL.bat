@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

set PYTHONPATH=%CLIENTS_PREFIX%\%PYTHON_LIBDIR%

call Mlog.bat -x sqlsample.py %MAPIPORT% %TSTDB%
