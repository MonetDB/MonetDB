@prompt # $t $g  

%SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\create_tables.flt.sql"

%SQL_CLIENT% < "%TSTSRCDIR%\check0.sql"

%SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\inserts.flt.sql"

%SQL_CLIENT% < "%TSTSRCDIR%\check0.sql"
