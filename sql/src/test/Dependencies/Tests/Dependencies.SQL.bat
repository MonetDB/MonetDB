@echo off

set SQL="mclient -lsql -umonetdb -Pmonetdb -h %HOST% -p %MAPIPORT%"
set SQL1="mclient -lsql -umonet_test -Ppass_test -h %HOST% -p %MAPIPORT%"

echo Dependencies between User and Schema
call %SQL% < %RELSRCDIR%\..\dependency_owner_schema_1.sql
echo done

call %SQL1% < %RELSRCDIR%/../dependency_owner_schema_2.sql
echo done

echo Dependencies between database objects
call %SQL% < %RELSRCDIR%/../dependency_DBobjects.sql
echo done

echo Dependencies between functions with same name
call %SQL% < %RELSRCDIR%/../dependency_functions.sql
echo done

echo Cleanup
call %SQL% < %RELSRCDIR%/../dependency_owner_schema_3.sql
echo done
