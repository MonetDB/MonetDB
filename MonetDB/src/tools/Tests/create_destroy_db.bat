@echo on
@prompt # $t $g  

%MCREATEDB% -db TestDB1
echo quit; | %MSERVER% -db TestDB1
@echo.
%MDESTROYDB% -db TestDB1

@set TSTDBHOME=%MONETFARM%
@set MONETFARM=

%MCREATEDB% -home %TSTDBHOME% -db TestDB2
echo quit; | %MSERVER% -home %TSTDBHOME% -db TestDB2
@echo.
%MDESTROYDB% -home %TSTDBHOME% -db TestDB2

