@echo off

prompt # $t $g  
echo on

perl %TSTSRCDIR\perl_int128.pl %MAPIPORT% %TSTDB%
