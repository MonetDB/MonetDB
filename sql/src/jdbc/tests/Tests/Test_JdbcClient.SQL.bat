@echo off

set CLASSPATH=%CLASSPATH%;%SQL_BUILD%/src/jdbc/MonetDB_JDBC.jar;%TSTBLDBASE%/src/jdbc/MonetDB_JDBC.jar;%SQL_PREFIX%/lib/MonetDB/java/MonetDB_JDBC.jar;%MONETDB_PREFIX%/lib/MonetDB/java/MonetDB_JDBC.jar;%TSTTRGBASE%/lib/MonetDB/java/MonetDB_JDBC.jar;%SQL_BUILD%/src/jdbc/tests;%TSTBLDBASE%/src/jdbc/tests;%SQL_PREFIX%/lib/sql/Tests;%MONETDB_PREFIX%/lib/sql/Tests;%TSTTRGBASE%/lib/sql/Tests

set LANG=en_US.UTF-8

call Mlog.bat -x java "%TST%" "jdbc:monetdb://%HOST%:%SQLPORT%/database?user=monetdb&password=monetdb"
