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
rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem extend the search path with our EXE and DLL folders
rem we depend on pthreadVCE.dll having been copied to the lib folder
set PATH=%MONETDB%\bin;%MONETDB%\lib;%MONETDB%\lib\bin;%PATH%

rem start the real client
"%MONETDB%\bin\stethoscope.exe" %1 %2 %3 %4 %5 %6 %7 %8

if ERRORLEVEL 1 pause
