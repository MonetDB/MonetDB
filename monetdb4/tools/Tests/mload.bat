@echo on
@prompt # $t $g  

@set NAME=%1

@set serverm=%MSERVER: -d8 = %

%MLOAD% %NAME%.data -db %TSTDB% -oid 1000000 -f %NAME%.format
%MSERVER% --dbname=%TSTDB% < %NAME%.mil
%serverm% --dbname=%TSTDB% < %NAME%.mil
