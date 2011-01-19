@echo off

if not '%1' == '-x' (
	echo # ..:..:.. .  %*
	goto :EOF
)

prompt # $t $g  
echo on
call %~2 %~3 %~4 %~5 %~6 %~7 %~8 %~9
