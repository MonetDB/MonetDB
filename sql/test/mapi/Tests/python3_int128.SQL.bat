@echo off

rem must be aligned with the installation directory chosen in
rem clients/python/test/Makefile.ag
set testpath=%TSTSRCDIR%

prompt # $t $g  

"%PYTHON%" "%testpath%/python_int128.py %MAPIPORT% %TSTDB% %MAPIHOST%"
