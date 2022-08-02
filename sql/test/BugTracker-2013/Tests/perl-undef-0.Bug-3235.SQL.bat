@echo off

prompt # $t $g  
echo on

perl "%TSTSRCDIR%\perl-undef-0.Bug-3235.pl" %MAPIPORT% %TSTDB%
