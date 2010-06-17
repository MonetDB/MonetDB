@echo off
rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem extend the search path with our EXE and DLL folders
rem we depend on pthreadVCE.dll having been copied to the lib folder
set PATH=%MONETDB%\bin;%MONETDB%\lib;%MONETDB%\lib\bin;%PATH%

if "%1"=="/STARTED-FROM-MENU" (
shift
if "%DOTMONETDBFILE%"=="" if exist "%MONETDB%\etc\.monetdb" set DOTMONETDBFILE=%MONETDB%\etc\.monetdb
)

rem start the real client
"%MONETDB%\bin\mclient.exe" %1 %2 %3 %4 %5 %6 %7 %8

if ERRORLEVEL 1 pause
