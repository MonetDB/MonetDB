@echo off

set CLASSPATH=%CLASSPATH%;%SQL_BUILD%/src/jdbc/MonetJDBC.jar;%TSTBLDBASE%/src/jdbc/MonetJDBC.jar;%SQL_PREFIX%/lib/MonetDB/java/MonetJDBC.jar;%MONETDB_PREFIX%/lib/MonetDB/java/MonetJDBC.jar;%TSTTRGBASE%/lib/MonetDB/java/MonetJDBC.jar;%SQL_BUILD%/src/jdbc/tests;%TSTBLDBASE%/src/jdbc/tests;%SQL_PREFIX%/lib/sql/Tests;%MONETDB_PREFIX%/lib/sql/Tests;%TSTTRGBASE%/lib/sql/Tests

call Mlog.bat -x java "%TST% jdbc:monetdb://%HOST%:%SQLPORT%/database?user=monetdb&password=monetdb"
