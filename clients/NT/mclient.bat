@REM This Source Code Form is subject to the terms of the Mozilla Public
@REM License, v. 2.0.  If a copy of the MPL was not distributed with this
@REM file, You can obtain one at http://mozilla.org/MPL/2.0/.
@REM
@REM Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

@echo off
rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem extend the search path with our EXE and DLL folders
set PATH=%MONETDB%\bin;%MONETDB%\lib\monetdb5;%PATH%

if not "%1"=="/STARTED-FROM-MENU" goto skip
shift
if "%DOTMONETDBFILE%"=="" if exist "%MONETDB%\etc\.monetdb" set DOTMONETDBFILE=%MONETDB%\etc\.monetdb
:skip

rem start the real client
"%MONETDB%\bin\mclient.exe" %1 %2 %3 %4 %5 %6 %7 %8

if ERRORLEVEL 1 pause
