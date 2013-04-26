set TSTDB=demo
set TSTHOSTNAME=localhost
set TSTUSERNAME=monetdb
set TSTPASSWORD=monetdb
set TSTDEBUG=no

nosetests ./runtests.py
nosetests ./test_control.py
