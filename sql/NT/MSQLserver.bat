@echo off

setlocal

rem figure out the folder name
set MONETDB=%~dp0

rem remove the final backslash from the path
set MONETDB=%MONETDB:~0,-1%

rem start the real server

if exist "%MONETDB%\M5server.bat" goto m5server
"%MONETDB%\Mserver.bat" --dbinit="module(sql_server); sql_server_start();" %*

:m5server
"%MONETDB%\M5server.bat" --dbinit="include sql; sql_start();" %*

endlocal