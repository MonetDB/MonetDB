@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

set PERLLIB=%CLIENTS_PREFIX%\%PERL_LIBDIR%

call Mlog.bat -x milsample.pl %MAPIPORT% %TSTDB%
