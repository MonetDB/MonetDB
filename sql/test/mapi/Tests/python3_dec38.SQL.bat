@echo off

rem must be aligned with the installation directory chosen in
rem clients/python/test/Makefile.ag
set testpath=%TSTSRCDIR%
rem ignore PYTHONPATH from Mtest, it is hardcoded to the dirs for the
rem Python that runs Mtest (currently always Python 3)
set PYTHONPATH=%testpath%;%PYTHON3PATH%

prompt # $t $g  

"%PYTHON3%" "%testpath%/python_dec38.py %MAPIPORT% %TSTDB% %MAPIHOST%"
