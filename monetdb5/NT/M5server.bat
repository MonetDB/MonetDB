@REM The contents of this file are subject to the MonetDB Public License
@REM Version 1.1 (the "License"); you may not use this file except in
@REM compliance with the License. You may obtain a copy of the License at
@REM http://www.monetdb.org/Legal/MonetDBLicense
@REM
@REM Software distributed under the License is distributed on an "AS IS"
@REM basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
@REM License for the specific language governing rights and limitations
@REM under the License.
@REM
@REM The Original Code is the MonetDB Database System.
@REM
@REM The Initial Developer of the Original Code is CWI.
@REM Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
@REM Copyright August 2008-2014 MonetDB B.V.
@REM All Rights Reserved.

@echo off

setlocal

rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem extend the search path with our EXE and DLL folders
rem we depend on pthreadVC2.dll having been copied to the lib folder
set PATH=%MONETDB%\bin;%MONETDB%\lib;%MONETDB%\lib\MonetDB5;%PATH%

rem prepare the arguments to mserver5 to tell it where to put the dbfarm

if "%APPDATA%" == "" goto usevar
rem if the APPDATA variable does exist, put the database there
set MONETDBDIR=%APPDATA%\MonetDB5
set MONETDBFARM="--dbpath=%MONETDBDIR%\dbfarm\demo"
goto skipusevar
:usevar
rem if the APPDATA variable does not exist, put the database in the
rem installation folder (i.e. default location, so no command line argument)
set MONETDBDIR=%MONETDB%\var\MonetDB5
set MONETDBFARM=
:skipusevar

rem the SQL log directory used to be in %MONETDBDIR%, but we now
rem prefer it inside the dbfarm, so move it there

if not exist "%MONETDBDIR%\sql_logs" goto skipmove
for /d %%i in ("%MONETDBDIR%"\sql_logs\*) do move "%%i" "%MONETDBDIR%\dbfarm"\%%~ni\sql_logs
rmdir "%MONETDBDIR%\sql_logs"
:skipmove

rem start the real server
"%MONETDB%\bin\mserver5.exe" --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" %MONETDBFARM% %*

if ERRORLEVEL 1 pause

endlocal
