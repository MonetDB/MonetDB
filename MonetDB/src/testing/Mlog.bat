@echo off

if not '%1' == '-x' (
	echo # ..:..:.. .  %*
	goto Done
)

set args=%*
prompt # $t $g  
echo on
call %args:~2%

:Done
