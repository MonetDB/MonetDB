@echo on
@prompt # $t $g  

@set NAME=%1
@set PRELUDE=%2 %3

%MSERVER% --dbname=%TSTDB% %PRELUDE% < %NAME%.mil
