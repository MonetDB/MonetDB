@echo off

rem must be aligned with the installation directory chosen in
rem clients/examples/python
set testpath=%TSTSRCBASE%\clients\examples\python

sqlsample.py %MAPIPORT% %TSTDB% %MAPIHOST%
