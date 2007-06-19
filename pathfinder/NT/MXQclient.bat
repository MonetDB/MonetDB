@rem figure out the folder name
@set MONETDB=%~dp0

@rem remove the final backslash from the path
@set MONETDB=%MONETDB:~0,-1%

@rem extend the search path with our EXE and DLL folders
@rem we depend on pthreadVCE.dll having been copied to the lib folder
@set PATH=%MONETDB%\bin;%MONETDB%\lib;%MONETDB%\lib\bin;%PATH%

@rem provide some helpful information about usage
@echo Type ? on a line by itself to get help

@rem start the real client
@"%MONETDB%\bin\MapiClient.exe" --set "prefix=%MONETDB%" --set "exec_prefix=%MONETDB%" -lxquery %*
