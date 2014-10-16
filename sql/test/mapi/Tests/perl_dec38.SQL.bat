@echo off

prompt # $t $g  
echo on

perl %TSTSRCDIR\perl_dec38.pl %MAPIPORT% %TSTDB%
