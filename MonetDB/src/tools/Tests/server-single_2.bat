@echo off

set NAME=%1
set PRELUDE=%2 %3

rem Mlog -x 
%MSERVER% -db %TSTDB% -single %PRELUDE% < %NAME%.mil
