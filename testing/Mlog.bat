@REM This Source Code Form is subject to the terms of the Mozilla Public
@REM License, v. 2.0.  If a copy of the MPL was not distributed with this
@REM file, You can obtain one at http://mozilla.org/MPL/2.0/.
@REM
@REM Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

@echo off

if not '%1' == '-x' (
	echo # ..:..:.. .  %*
	goto :EOF
)

prompt # $t $g  
echo on
call %~2 %~3 %~4 %~5 %~6 %~7 %~8 %~9
