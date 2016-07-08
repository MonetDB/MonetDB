@echo off
 
setlocal ENABLEEXTENSIONS
set KEY_NAME="HKEY_LOCAL_MACHINE\SOFTWARE\Python\PythonCore\2.7\InstallPath"
set VALUE_NAME=""
 
FOR /F "usebackq skip=2 tokens=1-3" %%A IN (`REG QUERY %KEY_NAME% /v %VALUE_NAME% 2^>nul`) DO (
    set ValueName=%%A
    set ValueType=%%B
    set ValueValue=%%C
)
IF defined ValueName (
    set LOCALPYTHONHOME=%ValueValue%
    set LOCALPYTHONPATH=%ValueValue%Lib
) ELSE (
    @echo %KEY_NAME%\%VALUE_NAME% not found.
)

IF defined LOCALPYTHONHOME (
    endlocal & (
        set PYTHONHOME=%LOCALPYTHONHOME%
        set PYTHONPATH=%LOCALPYTHONPATH%
        set MONETDBPYTHONUDF=embedded_py=true
        set "PATH=%LOCALPYTHONHOME%;%PATH%"
    )
) ELSE (
    @echo MonetDB/Python Disabled: Python 2.7 installation not found.
    endlocal
)
