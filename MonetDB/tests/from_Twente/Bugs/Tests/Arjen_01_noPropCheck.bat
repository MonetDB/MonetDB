@echo on
@prompt # $t $g  

@set NAME=%1
@set PRELUDE=%2 %3

@set serverm=%MSERVER: -d8 = %

%serverm% -db %TSTDB% < Arjen_01.milM
