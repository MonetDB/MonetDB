@echo on
@prompt # $t $g  

@set NAME=%1
@set PRELUDE=%2 %3

%MSERVER% -db %TSTDB% %PRELUDE% < %NAME%.mil
