@echo off

rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem extend the search path with our EXE and DLL folders
rem we depend on pthreadVCE.dll having been copied to the lib folder
set PATH=%MONETDB%\bin;%MONETDB%\lib;%PATH%

rem possibly move the database from a previous installation to our
rem currently preferred location, and prepare the arguments to Mserver
rem to tell it where that location is

set MONETDBFARM=
set SQLLOGDIR=
rem use the Application Data folder for our database
if "%ALLUSERSPROFILE%" == "" goto skip
set MONETDBFARM="--dbfarm=%ALLUSERSPROFILE%\Application Data\MonetDB\dbfarm"
set SQLLOGDIR=--set "sql_logdir=%ALLUSERSPROFILE%\Application Data\MonetDB\log"

rem if the database exists by the old name, move it
if not exist "%MONETDB%\var\MonetDB" goto skip
rem if the new path already exists, don't try moving the old to the new
if exist "%ALLUSERSPROFILE%\Application Data\MonetDB" goto skip
move "%MONETDB%\var\MonetDB" "%ALLUSERSPROFILE%\Application Data\MonetDB"
rmdir "%MONETDB%\var"

:skip

rem start the real server
"%MONETDB%\bin\Mserver.exe" --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" %MONETDBFARM% %SQLLOGDIR% %*
