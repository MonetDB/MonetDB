@echo off

setlocal

rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem extend the search path with our EXE and DLL folders
rem we depend on pthreadVC2.dll having been copied to the lib folder
set PATH=%MONETDB%\bin;%MONETDB%\lib;%MONETDB%\lib\MonetDB4;%PATH%

rem prepare the arguments to Mserver to tell it where to put the dbfarm

if "%APPDATA%" == "" (
rem if the APPDATA variable does not exist, put the database in the
rem installation folder (i.e. default location, so no command line argument)
set MONETDBDIR=%MONETDB%\var\MonetDB4
set MONETDBFARM=
) else (
rem if the APPDATA variable does exist, put the database there
set MONETDBDIR=%APPDATA%\MonetDB4
set MONETDBFARM="--dbfarm=%MONETDBDIR%\dbfarm"
)

rem the XQuery log directory used to be in %MONETDBDIR%, but we now
rem prefer it inside the dbfarm, so move it there

if exist "%MONETDBDIR%\xquery_logs" (
for /d %i in ("%MONETDBDIR%"\xquery_logs\*) do move "%i" "%MONETDBDIR%\dbfarm"\%~ni\xquery_logs
rmdir "%MONETDBDIR%\xquery_logs"
)

rem start the real server
"%MONETDB%\bin\Mserver.exe" --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" %MONETDBFARM% %*

if ERRORLEVEL 1 pause

endlocal
