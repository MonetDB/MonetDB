@echo off

set NAME=%1

set serverm=%MSERVER: -d8 = %

rem Mlog -x
%MLOAD% %NAME%.data -db %TSTDB% -oid 1000000 -f %NAME%.format
%MSERVER% -db %TSTDB% < %NAME%.mil
%serverm% -db %TSTDB% < %NAME%.mil
