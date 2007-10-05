@echo off

set SQL="mclient -lsql -umonetdb -Pmonetdb -h %HOST% -p %MAPIPORT%"
set SQL1="mclient -lsql -uskyserver -Pskyserver -h %HOST% -p %MAPIPORT%"

echo Create User
call %SQL% < %RELSRCDIR%\..\create_user.sql

echo tables
call %SQL1% < %RELSRCDIR%\..\..\..\sql\math.sql
call %SQL1% < %RELSRCDIR%\..\..\..\sql\cache.sql
call %SQL1% < %RELSRCDIR%\..\..\..\sql\ms_functions.sql
call %SQL1% < %RELSRCDIR%\..\Skyserver_tables.sql
call %SQL1% < %RELSRCDIR%\..\Skyserver_constraints.sql

echo views
call %SQL1% < %RELSRCDIR%\..\Skyserver_views.sql

echo functions
call %SQL1% < %RELSRCDIR%\..\Skyserver_functions.sql

echo Cleanup
call %SQL1% < %RELSRCDIR%\..\Skyserver_dropFunctions.sql
call %SQL1% < %RELSRCDIR%\..\Skyserver_dropMs_functions.sql
call %SQL1% < %RELSRCDIR%\..\Skyserver_dropMath.sql
call %SQL1% < %RELSRCDIR%\..\Skyserver_dropCache.sql
call %SQL1% < %RELSRCDIR%\..\Skyserver_dropViews.sql
call %SQL1% < %RELSRCDIR%\..\Skyserver_dropConstraints.sql
call %SQL1% < %RELSRCDIR%\..\Skyserver_dropTables.sql

echo Remove User
call %SQL% < %RELSRCDIR%\..\drop_user.sql
