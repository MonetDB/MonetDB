@echo on
@prompt # $t $g  

@set NAME=%1
@set PRELUDE=%2 %3

%MSERVER% --dbname=%TSTDB%         < %NAME%.1.mil
@echo  restart gives correct refcnt b from 0 to 1 
@echo  since b is part of a and persistent expected refcnt should be 2 
%MSERVER% --dbname=%TSTDB%         < %NAME%.2.mil

