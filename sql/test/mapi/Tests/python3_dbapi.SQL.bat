@echo off

prompt # $t $g  
echo on

rem must be aligned with the installation directory chosen in
rem clients/examples/python
set testpath=%TSTSRCBASE%\clients\examples\python
rem ignore PYTHONPATH from Mtest, it is hardcoded to the dirs for the
rem Python that runs Mtest (currently always Python 2)
set PYTHONPATH=%PYTHON3PATH%

"%PYTHON3%" "%testpath%/sqlsample.py" %MAPIPORT% %TSTDB% %MAPIHOST%
