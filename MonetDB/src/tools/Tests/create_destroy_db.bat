@echo off

rem Mlog -x
%MCREATEDB% -db TestDB1
rem Mlog   "echo 'quit;' | $MSERVER -db TestDB1"
echo quit; | %MSERVER% -db TestDB1
echo.
rem Mlog -x 
%MDESTROYDB% -db TestDB1

set TSTDBHOME=%MONETFARM%
set MONETFARM=

rem Mlog -x 
%MCREATEDB% -home %TSTDBHOME% -db TestDB2
rem Mlog   "echo 'quit;' | $MSERVER -home $TSTDBHOME -db TestDB2"
echo quit; | %MSERVER% -home %TSTDBHOME% -db TestDB2
echo.
rem Mlog -x 
%MDESTROYDB% -home %TSTDBHOME% -db TestDB2

