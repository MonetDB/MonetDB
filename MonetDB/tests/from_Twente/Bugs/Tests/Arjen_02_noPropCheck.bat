@echo on
@prompt # $t $g  

@set NAME=%1
@set PRELUDE=%2 %3

echo debugmask(2); > d2.mil
%MSERVER% -db %TSTDB% d2.mil Arjen_02.milM
