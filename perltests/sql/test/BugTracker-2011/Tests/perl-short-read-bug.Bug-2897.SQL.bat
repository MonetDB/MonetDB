@echo off

prompt # $t $g  
echo on

perl "%TSTSRCDIR%\perl-short-read-bug.Bug-2897.pl" %MAPIPORT% %TSTDB% %HOST%
