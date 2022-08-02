@echo off

prompt # $t $g  
echo on

@rem must use perl here since file association of .pl files not correct
@rem it doesn't propagate command arguments
perl "%BINDIR%\sqlsample.pl" %MAPIPORT% %TSTDB%
