@echo off

rem must be aligned with the installation directory chosen in
rem clients/python/test/Makefile.ag
set testpath=%TSTSRCBASE%\clients\python3\test
rem ignore PYTHONPATH from Mtest, it is hardcoded to the dirs for the
rem Python that runs Mtest (currently always Python 2)
set PYTHONPATH=%testpath%;%PYTHON3PATH%

prompt # $t $g  
echo on

%PYTHON3% "%testpath%/runtests.py"
