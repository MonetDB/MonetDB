@echo off

set CLASSPATH=%CLASSPATH%;%SQL_BUILD%/src/jdbc/MonetDB_JDBC.jar;%TSTBLDBASE%/src/jdbc/MonetDB_JDBC.jar;%SQL_PREFIX%/share/MonetDB/lib/MonetDB_JDBC.jar;%MONETDB_PREFIX%/share/MonetDB/lib/MonetDB_JDBC.jar;%TSTTRGBASE%/share/MonetDB/lib/MonetDB_JDBC.jar;%SQL_BUILD%/src/jdbc/tests;%TSTBLDBASE%/src/jdbc/tests;%SQL_PREFIX%/share/sql/Tests;%MONETDB_PREFIX%/share/sql/Tests;%TSTTRGBASE%/share/sql/Tests

call Mlog.bat -x java "%TST%" "jdbc:monetdb://%HOST%:%SQLPORT%/database?user=monetdb&password=monetdb"
