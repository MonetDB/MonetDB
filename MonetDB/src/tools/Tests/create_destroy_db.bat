@echo on
@prompt # $t $g  

%MCREATEDB% -db TestDB1
echo quit; | %MSERVER% -db TestDB1
@echo.
%MDESTROYDB% -db TestDB1

