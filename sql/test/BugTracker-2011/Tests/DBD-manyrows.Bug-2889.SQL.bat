@echo off

prompt # $t $g  
echo on

perl "%TSTSRCDIR%\DBD-manyrows.Bug-2889.pl" %MAPIPORT% %TSTDB%
