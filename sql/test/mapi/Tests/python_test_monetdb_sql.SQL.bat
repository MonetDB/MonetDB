@echo off

rem must be aligned with the installation directory chosen in
rem clients/python/test/Makefile.ag
set testpath=%TSTSRCBASE%\..\clients\python\test
set PYTHONPATH=%testpath%;%PYTHONPATH%

prompt # $t $g  
echo on

python "%testpath%/runtests.py"
