@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

set PERLLIB=%CLIENTS_PREFIX%\%PERL_LIBDIR%

prompt # $t $g  
echo on

milsample.pl %MAPIPORT% %TSTDB%
