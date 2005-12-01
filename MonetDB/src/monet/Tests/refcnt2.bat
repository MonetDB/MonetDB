@echo on
@prompt # $t $g  

@set NAME=%1
@set PRELUDE=%2 %3

%MSERVER% --dbname=%TSTDB%         < %NAME%.1.mil
%MSERVER% --dbname=%TSTDB%         < %NAME%.2.mil

