@echo off

call monetdb-clients-config --internal

rem must be aligned with the installation directory chosen in
rem clients/src/python/test/Makefile.ag
set testpath=%pkglibdir%\Tests
set PYTHONPATH=%testpath%;%PYTHONPATH%

prompt # $t $g  
echo on

python "%testpath%\dbapi20_monetdb.py"
