@echo off

prompt # $t $g  
echo on

perl "%TSTSRCDIR%\perl-table_info.Bug-2885.pl" %MAPIPORT% %TSTDB%
