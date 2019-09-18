@prompt # $t $g
@echo on

"%PYTHON3%" "%TSTSRCBASE%/%TSTDIR%/sqllogictest.py" --host=localhost --port=%MAPIPORT% --database=%TSTDB% "%TSTSRCBASE%/%TSTDIR%/select4.test"
