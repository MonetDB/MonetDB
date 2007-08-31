@echo off

rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem extend the search path with our EXE and DLL folders
rem we depend on pthreadVCE.dll having been copied to the lib folder
set PATH=%MONETDB%\bin;%MONETDB%\lib;%MONETDB%\lib\bin;%PATH%

rem start the real client
set XMLFILE=%TEMP%\MonetDB-XQuery-%RANDOM%.xml
"%MONETDB%\bin\mclient.exe"  --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" -lxquery -oxml %* > "%XMLFILE%" && "%XMLFILE%"

rem add a short sleep to make sure that there was time to open the
rem file before we delete it
sleep 10

del "%XMLFILE%"
