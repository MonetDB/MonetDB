@echo off

setlocal

rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem extend the search path with our EXE and DLL folders
rem we depend on pthreadVCE.dll having been copied to the lib folder
set PATH=%MONETDB%\bin;%MONETDB%\lib;%MONETDB%\lib\MonetDB;%PATH%

rem possibly move the database from a previous installation to our
rem currently preferred location, and prepare the arguments to Mserver
rem to tell it where that location is

set MONETDBDIR=
set MONETDBFARM=
set SQLLOGDIR=
rem use the Application Data folder for our database

rem if installed for just the current user, the file
rem %APPDATA%\MonetDB\VERSION was created by the installer, so set
rem MONETDBDIR accordingly.
rem if ALLUSERSPROFILE and APPDATA variables don't exist, forget about
rem this whole exercise and use the default (i.e. %MONETDB\var\MonetDB).

if "%ALLUSERSPROFILE%" == "" goto skip
if "%APPDATA%" == "" goto skip

if exist "%APPDATA%\MonetDB\VERSION" set MONETDBDIR=%APPDATA%\MonetDB
if "%MONETDBDIR%" == "" set MONETDBDIR=%ALLUSERSPROFILE%\Application Data\MonetDB

set MONETDBFARM="--dbfarm=%MONETDBDIR%\dbfarm"
set SQLLOGDIR=--set "sql_logdir=%MONETDBDIR%\log"

rem if the database exists by the old name, move it
if not exist "%MONETDB%\var\MonetDB" goto skip
rem if the new path already exists, don't try moving the old to the new
if exist "%MONETDBDIR%" goto skip
move "%MONETDB%\var\MonetDB" "%MONETDBDIR%"
rmdir "%MONETDB%\var"

:skip

rem start the real server
"%MONETDB%\bin\Mserver.exe" --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" %MONETDBFARM% %SQLLOGDIR% %*

endlocal
