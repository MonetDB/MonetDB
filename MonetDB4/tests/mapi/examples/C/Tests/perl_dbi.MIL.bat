@echo off

set PATH=%CLIENTS_PREFIX%\lib\MonetDB\Tests;%PATH%

if "%PERL_LIBDIR%"=="" set PERL_LIBDIR=share\MonetDB\perl
set PERLLIB=%CLIENTS_PREFIX%\%PERL_LIBDIR%

prompt # $t $g  
echo on

milsample.pl %MAPIPORT% %TSTDB%
