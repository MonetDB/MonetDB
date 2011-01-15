@prompt # $t $g  
@echo on

%SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\create_tables.flt.sql"

%SQL_CLIENT% < "%TSTSRCDIR%\check0.sql"

%SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\insert_MODEL.flt.sql"

%SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\insert_ATOM.flt.sql"

%SQL_CLIENT% < "%TSTSRCBASE%\%TSTDIR%\insert_BOND.flt.sql"

%SQL_CLIENT% < "%TSTSRCDIR%\check0.sql"
