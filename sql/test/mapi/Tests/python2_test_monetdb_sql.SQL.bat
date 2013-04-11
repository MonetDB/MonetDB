@echo off

rem must be aligned with the installation directory chosen in
rem clients/python/test/Makefile.ag
set testpath=%TSTSRCBASE%\clients\python2\test
rem ignore PYTHONPATH from Mtest, it is hardcoded to the dirs for the
rem Python that runs Mtest (currently always Python 2)
set PYTHONPATH=%testpath%;%PYTHON2PATH%

prompt # $t $g  
echo on

%PYTHON2% "%testpath%/runtests.py"
