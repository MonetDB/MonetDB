@echo off

set NAME=%1
set PRELUDE=%2 %3

set serverm=%MSERVER: -d8 = %
rem Mlog -x 
%serverm% -db %TSTDB% < Arjen_02.milM
