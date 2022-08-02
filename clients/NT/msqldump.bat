@REM This Source Code Form is subject to the terms of the Mozilla Public
@REM License, v. 2.0.  If a copy of the MPL was not distributed with this
@REM file, You can obtain one at http://mozilla.org/MPL/2.0/.
@REM
@REM Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

@rem figure out the folder name
@set MONETDB=%~dp0

@rem remove the final backslash from the path
@set MONETDB=%MONETDB:~0,-1%

@rem extend the search path with our EXE and DLL folders
@set PATH=%MONETDB%\bin;%MONETDB%\lib\monetdb5;%PATH%

@rem start the real client
@"%MONETDB%\bin\msqldump.exe" %*

@if ERRORLEVEL 1 pause
